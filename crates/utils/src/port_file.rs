use std::{env, path::PathBuf};

use serde::{Deserialize, Serialize};
use tokio::fs;

pub const INSTANCE_ID_ENV: &str = "VIBE_KANBAN_INSTANCE_ID";

#[derive(Debug, Serialize, Deserialize)]
pub struct PortInfo {
    pub main_port: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview_proxy_port: Option<u16>,
}

pub async fn write_port_file_with_proxy(
    main_port: u16,
    preview_proxy_port: Option<u16>,
) -> std::io::Result<PathBuf> {
    let app_name = instance_name("vibe-kanban")?;
    let dir = env::temp_dir().join(&app_name);
    let path = dir.join(format!("{app_name}.port"));
    let port_info = PortInfo {
        main_port,
        preview_proxy_port,
    };
    let content = serde_json::to_string(&port_info)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    tracing::debug!("Writing ports {:?} to {:?}", port_info, path);
    fs::create_dir_all(&dir).await?;
    fs::write(&path, content).await?;
    Ok(path)
}

pub async fn read_port_file(app_name: &str) -> std::io::Result<u16> {
    read_port_info(app_name).await.map(|info| info.main_port)
}

pub async fn read_port_info(app_name: &str) -> std::io::Result<PortInfo> {
    let app_name = instance_name(app_name)?;
    let dir = env::temp_dir().join(&app_name);
    let path = dir.join(format!("{app_name}.port"));
    tracing::debug!("Reading port from {:?}", path);

    let content = fs::read_to_string(&path).await?;

    if let Ok(port_info) = serde_json::from_str::<PortInfo>(&content) {
        return Ok(port_info);
    }

    let port: u16 = content
        .trim()
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    Ok(PortInfo {
        main_port: port,
        preview_proxy_port: None,
    })
}

fn instance_name(default: &str) -> std::io::Result<String> {
    let name = env::var(INSTANCE_ID_ENV).unwrap_or_else(|_| default.to_string());
    if !valid_instance_name(&name) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{INSTANCE_ID_ENV} may contain only ASCII letters, numbers, '-', '_' and '.'"),
        ));
    }
    Ok(name)
}

fn valid_instance_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

#[cfg(test)]
mod tests {
    use super::valid_instance_name;

    #[test]
    fn instance_names_cannot_escape_the_temp_directory() {
        assert!(valid_instance_name("vibe-kanban-canary_1.2"));
        assert!(!valid_instance_name("../production"));
        assert!(!valid_instance_name("canary/one"));
        assert!(!valid_instance_name(""));
    }
}
