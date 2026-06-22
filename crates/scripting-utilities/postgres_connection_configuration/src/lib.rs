/// Config utility for connecting to multiple DBs in the same cluster
// JOSH - I'm not sure if this really warrants a crate, but it seems like if we
//        ever change this it'll be annoying to hunt down everything ¯\_(ツ)_/¯

#[derive(Clone)]
pub struct ConnectionConfig {
    pub host: Option<String>,
    pub port: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub database: Option<String>,
}

impl ConnectionConfig {
    pub fn with_db(&self, database: &str) -> ConnectionConfig {
        let mut new = self.clone();
        new.database = Some(database.to_owned());
        new
    }

    /// get a config string we can use to connect to the db
    pub fn config_string(&self) -> String {
        use std::fmt::Write;

        let ConnectionConfig {
            host,
            port,
            user,
            password,
            database,
        } = self;

        let mut config = String::new();
        if let Some(host) = host {
            let _ = write!(&mut config, "host={host} ");
        }
        if let Some(port) = port {
            let _ = write!(&mut config, "port={port} ");
        }
        let _ = match user {
            Some(user) => write!(&mut config, "user={user} "),
            None => write!(&mut config, "user=postgres "),
        };
        if let Some(password) = password {
            let _ = write!(&mut config, "password={password} ");
        }
        if let Some(database) = database {
            let _ = write!(&mut config, "dbname={database} ");
        }
        config
    }
}
