//! Databases role (Go `digitalocean_db.go`) with godo's `Databases.List` (`/v2/databases`; its response
//! root has no `links`, so pages are walked until an empty one).
use std::sync::Arc;

use json::Json;
use model::labelset::{ADDRESS_LABEL, LabelSet};

use super::{GodoClient, ListOptions, add_options};
use crate::cancel::CancelToken;
use crate::gojson::{Dec, key_is, sub};
use crate::sdutil::join_host_port;
use crate::targetgroup::Group;

const DB_LABEL_ID: &str = "__meta_digitalocean_db_id";
const DB_LABEL_NAME: &str = "__meta_digitalocean_db_name";
const DB_LABEL_ENGINE: &str = "__meta_digitalocean_db_engine";
const DB_LABEL_VERSION: &str = "__meta_digitalocean_db_version";
const DB_LABEL_STATUS: &str = "__meta_digitalocean_db_status";
const DB_LABEL_REGION: &str = "__meta_digitalocean_db_region";
const DB_LABEL_SIZE: &str = "__meta_digitalocean_db_size";
const DB_LABEL_NUM_NODES: &str = "__meta_digitalocean_db_num_nodes";
const DB_LABEL_HOST: &str = "__meta_digitalocean_db_host";
const DB_LABEL_PRIVATE_HOST: &str = "__meta_digitalocean_db_private_host";
const DB_LABEL_TAG_PREFIX: &str = "__meta_digitalocean_db_tag_";

/// Go `databasesDiscovery`.
pub struct DatabasesDiscovery {
    pub client: GodoClient,
    pub(super) port: i64,
}

/// The fields of `godo.Database` the discovery reads.
#[derive(Default)]
struct Database {
    id: String,
    name: String,
    engine: String,
    version: String,
    status: String,
    region: String,
    size: String,
    num_nodes: i64,
    /// Host of `connection` / `private_connection` (`None`: nil).
    connection: Option<String>,
    private_connection: Option<String>,
    tags: Vec<String>,
}

fn decode_connection(d: &mut Dec, v: &Json, path: &str, dst: &mut Option<String>) {
    match d.obj(v, "Database", path, "godo.DatabaseConnection") {
        Some(o) => {
            let host = dst.get_or_insert_with(String::new);
            for (k, x) in o {
                if key_is(k, "host") {
                    d.str(x, host, "DatabaseConnection", &sub(path, k));
                }
            }
        }
        None if v.is_null() => *dst = None,
        None => {}
    }
}

fn decode_database(d: &mut Dec, v: &Json) -> Database {
    let mut db = Database::default();
    let Some(o) = d.obj(v, "databasesRoot", "databases", "godo.Database") else { return db };
    let st = "Database";
    for (k, e) in o {
        let p = sub("databases", k);
        let s = if key_is(k, "id") {
            &mut db.id
        } else if key_is(k, "name") {
            &mut db.name
        } else if key_is(k, "engine") {
            &mut db.engine
        } else if key_is(k, "version") {
            &mut db.version
        } else if key_is(k, "status") {
            &mut db.status
        } else if key_is(k, "region") {
            &mut db.region
        } else if key_is(k, "size") {
            &mut db.size
        } else {
            if key_is(k, "num_nodes") {
                d.int(e, &mut db.num_nodes, st, &p, "int");
            } else if key_is(k, "connection") {
                decode_connection(d, e, &p, &mut db.connection);
            } else if key_is(k, "private_connection") {
                decode_connection(d, e, &p, &mut db.private_connection);
            } else if key_is(k, "tags") {
                d.str_slice(e, &mut db.tags, st, &p);
            }
            continue;
        };
        d.str(e, s, st, &p);
    }
    db
}

impl DatabasesDiscovery {
    /// Go `Databases.List`.
    fn list(&self, opts: ListOptions) -> Result<Vec<Database>, String> {
        let v = self.client.get(&add_options("/v2/databases", opts)?)?;
        let mut d = Dec::new();
        let mut out = Vec::new();
        if let Some(o) = d.obj(&v, "", "", "godo.databasesRoot") {
            for (k, e) in o {
                if key_is(k, "databases")
                    && let Some(a) = d.arr(e, "databasesRoot", "databases", "[]godo.Database")
                {
                    out = a.iter().map(|x| decode_database(&mut d, x)).collect();
                }
            }
        }
        d.finish()?;
        Ok(out)
    }

    /// Go `listClusters`.
    fn list_clusters(&self) -> Result<Vec<Database>, String> {
        let mut clusters = Vec::new();
        let mut opts = ListOptions { page: 1, per_page: 100 };
        loop {
            let page = self.list(opts).map_err(|e| format!("error while listing database clusters page {}: {e}", opts.page))?;
            if page.is_empty() {
                break;
            }
            clusters.extend(page);
            // godo's databases root has no links, so the page number always goes up by one.
            opts.page += 1;
        }
        Ok(clusters)
    }

    /// Go `refresh`.
    pub fn refresh(&mut self, _ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let mut tg = Group { source: "DigitalOcean Databases".to_string(), ..Default::default() };
        let port = (self.port as u64).to_string();
        for c in self.list_clusters()? {
            let mut labels = LabelSet::from_pairs(&[
                (DB_LABEL_ID, &c.id),
                (DB_LABEL_NAME, &c.name),
                (DB_LABEL_ENGINE, &c.engine),
                (DB_LABEL_VERSION, &c.version),
                (DB_LABEL_STATUS, &c.status),
                (DB_LABEL_REGION, &c.region),
                (DB_LABEL_SIZE, &c.size),
                (DB_LABEL_NUM_NODES, &c.num_nodes.to_string()),
            ]);
            let mut host = String::new();
            if let Some(h) = &c.private_connection {
                host = h.clone();
                labels.insert(DB_LABEL_PRIVATE_HOST.to_string(), host.clone());
            }
            if let Some(h) = &c.connection {
                labels.insert(DB_LABEL_HOST.to_string(), h.clone());
                if host.is_empty() {
                    host = h.clone();
                }
            }
            if !host.is_empty() {
                labels.insert(ADDRESS_LABEL.to_string(), join_host_port(&host, &port));
            }
            for tag in &c.tags {
                labels.insert(format!("{DB_LABEL_TAG_PREFIX}{tag}"), "true".to_string());
            }
            tg.targets.push(labels);
        }
        Ok(vec![Arc::new(tg)])
    }
}
