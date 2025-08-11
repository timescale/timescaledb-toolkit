/// Config utility for connecting to multiple DBs in the same cluster
// JOSH - I'm not sure if this really warrants a crate, but it seems like if we
//        ever change this it'll be annoying to hunt down everything ¯\_(ツ)_/¯

#[derive(Copy, Clone)]
pub struct ConnectionConfig<'s> {
    pub host: Option<&'s str>,
    pub port: Option<&'s str>,
    pub user: Option<&'s str>,
    pub password: Option<&'s str>,
    pub database: Option<&'s str>,
}

impl<'s> ConnectionConfig<'s> {
    pub fn with_db<'d>(&self, database: &'d str) -> ConnectionConfig<'d>
    where
        's: 'd,
    {
        ConnectionConfig {
            database: Some(database),
            ..*self
        }
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
