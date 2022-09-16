use std::{collections::HashMap, ffi::OsStr, fs};

use pulldown_cmark::{
    CodeBlockKind::Fenced,
    CowStr, Event, Parser,
    Tag::{CodeBlock, Heading},
};
use semver::Version;

#[derive(Debug, PartialEq, Eq)]
#[must_use]
pub struct TestFile {
    name: String,
    stateless: bool,
    pub tests: Vec<Test>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
#[must_use]
pub struct Test {
    pub location: String,
    pub header: String,
    pub text: String,
    pub output: Vec<Vec<String>>,
    transactional: bool,
    ignore_output: bool,
    pub precision_limits: HashMap<usize, usize>,
    pub creation: bool,
    pub validation: bool,
    pub min_toolkit_version: Option<Version>,
}

pub fn extract_tests(root: &str) -> Vec<TestFile> {
    // TODO handle when root is a file
    let mut all_tests = vec![];
    let walker = walkdir::WalkDir::new(root)
        .follow_links(true)
        .sort_by(|a, b| a.path().cmp(b.path()));
    for entry in walker {
        let entry = entry.unwrap();
        if !entry.file_type().is_file() {
            continue;
        }

        if entry.path().extension() != Some(OsStr::new("md")) {
            continue;
        }

        let contents = fs::read_to_string(entry.path()).unwrap();

        let tests = extract_tests_from_string(&*contents, &*entry.path().to_string_lossy());
        if !tests.tests.is_empty() {
            all_tests.push(tests)
        }
    }
    all_tests
}
// parsers the grammar `(heading* (test output?)*)*`
pub fn extract_tests_from_string(s: &str, file_stem: &str) -> TestFile {
    let mut parser = Parser::new(s).into_offset_iter().peekable();
    let mut heading_stack: Vec<String> = vec![];
    let mut tests = vec![];

    let mut last_test_seen_at = 0;
    let mut lines_seen = 0;

    let mut stateless = true;

    // consume the parser until an tag is reached, performing an action on each text
    macro_rules! consume_text_until {
        ($parser: ident yields $end: pat => $action: expr) => {
            for (event, _) in &mut parser {
                match event {
                    Event::Text(text) => $action(text),
                    $end => break,
                    _ => (),
                }
            }
        };
    }

    'block_hunt: while let Some((event, span)) = parser.next() {
        match event {
            // we found a heading, add it to the stack
            Event::Start(Heading(level)) => {
                heading_stack.truncate(level as usize - 1);
                let mut header = "`".to_string();
                consume_text_until!(parser yields Event::End(Heading(..)) =>
                    |text: CowStr| header.push_str(&*text)
                );
                header.truncate(header.trim_end().len());
                header.push('`');
                heading_stack.push(header);
            }

            // we found a code block, if it's a test add the test
            Event::Start(CodeBlock(Fenced(ref info))) => {
                let code_block_info = parse_code_block_info(info);

                // non-test code block, consume it and continue looking
                if let BlockKind::Other = code_block_info.kind {
                    for (event, _) in &mut parser {
                        if let Event::End(CodeBlock(Fenced(..))) = event {
                            break;
                        }
                    }
                    continue 'block_hunt;
                }

                let current_line = {
                    let offset = span.start;
                    lines_seen += bytecount::count(&s.as_bytes()[last_test_seen_at..offset], b'\n');
                    last_test_seen_at = offset;
                    lines_seen + 1
                };

                if let BlockKind::Output = code_block_info.kind {
                    panic!(
                        "found output with no test test.\n{}:{} {:?}",
                        file_stem, current_line, heading_stack
                    )
                }

                assert!(matches!(code_block_info.kind, BlockKind::Sql));

                stateless &= code_block_info.transactional;
                let mut test = Test {
                    location: format!("{}:{}", file_stem, current_line),
                    header: if heading_stack.is_empty() {
                        "<root>".to_string()
                    } else {
                        heading_stack.join("::")
                    },
                    text: String::new(),
                    output: Vec::new(),
                    transactional: code_block_info.transactional,
                    ignore_output: code_block_info.ignore_output,
                    precision_limits: code_block_info.precision_limits,
                    min_toolkit_version: code_block_info.min_toolkit_version,
                    creation: code_block_info.creation,
                    validation: code_block_info.validation,
                };

                // consume the lines of the test
                consume_text_until!(parser yields Event::End(CodeBlock(Fenced(..))) =>
                    |text: CowStr| test.text.push_str(&*text)
                );

                // search to see if we have output
                loop {
                    match parser.peek() {
                        // we found a code block, is it output?
                        Some((Event::Start(CodeBlock(Fenced(info))), _)) => {
                            let code_block_info = parse_code_block_info(info);
                            match code_block_info.kind {
                                // non-output, continue at the top
                                BlockKind::Sql | BlockKind::Other => {
                                    tests.push(test);
                                    continue 'block_hunt;
                                }

                                // output, consume it
                                BlockKind::Output => {
                                    if !test.precision_limits.is_empty()
                                        && !code_block_info.precision_limits.is_empty()
                                    {
                                        panic!(
                                            "cannot have precision limits on both test and output.\n{}:{} {:?}",
                                            file_stem, current_line, heading_stack
                                        )
                                    }
                                    test.precision_limits = code_block_info.precision_limits;
                                    let _ = parser.next();
                                    break;
                                }
                            }
                        }

                        // test must be over, continue at the top
                        Some((Event::Start(CodeBlock(..)), _))
                        | Some((Event::Start(Heading(..)), _)) => {
                            tests.push(test);
                            continue 'block_hunt;
                        }

                        // EOF, we're done
                        None => {
                            tests.push(test);
                            break 'block_hunt;
                        }

                        // for now we allow text between the test and it's output
                        // TODO should/can we forbid this?
                        _ => {
                            let _ = parser.next();
                        }
                    };
                }

                // consume the output
                consume_text_until!(parser yields Event::End(CodeBlock(Fenced(..))) =>
                    |text: CowStr| {
                        let rows = text.split('\n').skip(2).filter(|s| !s.is_empty()).map(|s|
                            s.split('|').map(|s| s.trim().to_string()).collect::<Vec<_>>()
                        );
                        test.output.extend(rows);
                    }
                );

                tests.push(test);
            }

            _ => (),
        }
    }
    TestFile {
        name: file_stem.to_string(),
        stateless,
        tests,
    }
}

struct CodeBlockInfo {
    kind: BlockKind,
    transactional: bool,
    ignore_output: bool,
    precision_limits: HashMap<usize, usize>,
    min_toolkit_version: Option<Version>,
    creation: bool,
    validation: bool,
}

#[derive(Clone, Copy)]
enum BlockKind {
    Sql,
    Output,
    Other,
}

fn parse_code_block_info(info: &str) -> CodeBlockInfo {
    let tokens = info.split(',');

    let mut info = CodeBlockInfo {
        kind: BlockKind::Other,
        transactional: true,
        ignore_output: false,
        precision_limits: HashMap::new(),
        min_toolkit_version: None,
        creation: false,
        validation: false,
    };

    for token in tokens {
        match token.trim() {
            "ignore" => {
                if let BlockKind::Sql = info.kind {
                    info.kind = BlockKind::Other;
                }
            }
            "non-transactional" => info.transactional = false,
            "ignore-output" => info.ignore_output = true,
            m if m.starts_with("min-toolkit-version") => {
                // TODO Can we assume that version is greater than 1.10.1 since current tests don't have a min version? This means we ccan skip edge cases of 1.4/1.10.0-dev/etc.
                info.min_toolkit_version =
                    Some(Version::parse(token.trim_start_matches("min-toolkit-version=")).unwrap())
            } // not great, shouldn't assume they typed in a valid version. fix later
            "creation" => info.creation = true,
            "validation" => info.validation = true,
            "output" => info.kind = BlockKind::Output,
            s if s.to_ascii_lowercase() == "sql" => info.kind = BlockKind::Sql,
            p if p.starts_with("precision") => {
                // syntax `precision(col: bytes)`
                let precision_err =
                    || -> ! { panic!("invalid syntax for `precision(col: bytes)` found `{}`", p) };
                let arg = &p["precision".len()..];
                if arg.as_bytes().first() != Some(&b'(') || arg.as_bytes().last() != Some(&b')') {
                    precision_err()
                }
                let arg = &arg[1..arg.len() - 1];
                let args: Vec<_> = arg.split(':').collect();
                if args.len() != 2 {
                    precision_err()
                }
                let column = args[0].trim().parse().unwrap_or_else(|_| precision_err());
                let length = args[1].trim().parse().unwrap_or_else(|_| precision_err());
                let old = info.precision_limits.insert(column, length);
                if old.is_some() {
                    panic!("duplicate precision for column {}", column)
                }
            }
            _ => {}
        }
    }

    info
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use semver::{BuildMetadata, Prerelease, Version};

    #[test]
    fn extract() {
        use super::{Test, TestFile};

        let file = r##"
# Test Parsing
```SQL,creation
select * from foo
```
```output
```

```SQL,creation
select * from multiline
```
```output
 ?column?
----------
    value
```

## ignored
```SQL,ignore,creation
select * from foo
```

## non-transactional,creation
```SQL,non-transactional,creation
select * from bar
```
```output, precision(1: 3)
 a | b
---+---
 1 | 2
```

## no output
```SQL,ignore-output,creation
select * from baz
```

## end by header
```SQL,creation
select * from quz
```

## end by file
```SQL,creation
select * from qat
```

## has a min-toolkit-version
```SQL,creation,min-toolkit-version=1.10.1
select * from qat
```
"##;

        let tests = super::extract_tests_from_string(file, "/test/file.md");
        let expected = TestFile {
            name: "/test/file.md".to_string(),
            stateless: false,
            tests: vec![
                Test {
                    location: "/test/file.md:3".to_string(),
                    header: "`Test Parsing`".to_string(),
                    text: "select * from foo\n".to_string(),
                    output: vec![],
                    transactional: true,
                    ignore_output: false,
                    precision_limits: HashMap::new(),
                    creation: true,
                    min_toolkit_version: None,
                    validation: false,
                },
                Test {
                    location: "/test/file.md:9".to_string(),
                    header: "`Test Parsing`".to_string(),
                    text: "select * from multiline\n".to_string(),
                    output: vec![vec!["value".to_string()]],
                    transactional: true,
                    ignore_output: false,
                    precision_limits: HashMap::new(),
                    creation: true,
                    min_toolkit_version: None,
                    validation: false,
                },
                Test {
                    location: "/test/file.md:24".to_string(),
                    header: "`Test Parsing`::`non-transactional,creation`".to_string(),
                    text: "select * from bar\n".to_string(),
                    output: vec![vec!["1".to_string(), "2".to_string()]],
                    transactional: false,
                    ignore_output: false,
                    precision_limits: [(1, 3)].iter().cloned().collect(),
                    creation: true,
                    min_toolkit_version: None,
                    validation: false,
                },
                Test {
                    location: "/test/file.md:34".to_string(),
                    header: "`Test Parsing`::`no output`".to_string(),
                    text: "select * from baz\n".to_string(),
                    output: vec![],
                    transactional: true,
                    ignore_output: true,
                    precision_limits: HashMap::new(),
                    creation: true,
                    min_toolkit_version: None,
                    validation: false,
                },
                Test {
                    location: "/test/file.md:39".to_string(),
                    header: "`Test Parsing`::`end by header`".to_string(),
                    text: "select * from quz\n".to_string(),
                    output: vec![],
                    transactional: true,
                    ignore_output: false,
                    precision_limits: HashMap::new(),
                    creation: true,
                    min_toolkit_version: None,
                    validation: false,
                },
                Test {
                    location: "/test/file.md:44".to_string(),
                    header: "`Test Parsing`::`end by file`".to_string(),
                    text: "select * from qat\n".to_string(),
                    output: vec![],
                    transactional: true,
                    ignore_output: false,
                    precision_limits: HashMap::new(),
                    creation: true,
                    min_toolkit_version: None,
                    validation: false,
                },
                Test {
                    location: "/test/file.md:49".to_string(),
                    header: "`Test Parsing`::`has a min-toolkit-version`".to_string(),
                    text: "select * from qat\n".to_string(),
                    output: vec![],
                    transactional: true,
                    ignore_output: false,
                    precision_limits: HashMap::new(),
                    creation: true,
                    min_toolkit_version: Some(Version {
                        major: 1,
                        minor: 10,
                        patch: 1,
                        pre: Prerelease::EMPTY,
                        build: BuildMetadata::EMPTY,
                    }),
                    validation: false,
                },
            ],
        };
        assert!(
            tests == expected,
            "left: {:#?}\n right: {:#?}",
            tests,
            expected
        );
    }
}
