use std::{fs, path::Path, path::PathBuf};

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn cargo_manifests(directory: &Path, manifests: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("read repository directory") {
        let entry = entry.expect("read repository entry");
        let path = entry.path();
        if path.is_dir() {
            let name = entry.file_name();
            if name != "target" && name != ".git" {
                cargo_manifests(&path, manifests);
            }
        } else if entry.file_name() == "Cargo.toml" {
            manifests.push(path);
        }
    }
}

#[test]
fn path_dependencies_stay_inside_the_repository() {
    let root = repository_root()
        .canonicalize()
        .expect("canonical repository root");
    let mut manifests = Vec::new();
    cargo_manifests(&root, &mut manifests);

    for manifest in manifests {
        let contents = fs::read_to_string(&manifest).expect("read Cargo manifest");
        for line in contents.lines().filter(|line| line.contains("path")) {
            let Some((_, value)) = line.split_once("path") else {
                continue;
            };
            let Some((_, quoted)) = value.split_once('"') else {
                continue;
            };
            let Some((dependency_path, _)) = quoted.split_once('"') else {
                continue;
            };
            let dependency_path = Path::new(dependency_path);
            assert!(
                !dependency_path.is_absolute(),
                "{} contains absolute path dependency {}",
                manifest.display(),
                dependency_path.display()
            );
            let resolved = manifest
                .parent()
                .expect("manifest directory")
                .join(dependency_path)
                .canonicalize()
                .expect("path dependency resolves");
            assert!(
                resolved.starts_with(&root),
                "{} contains cross-repository path dependency {}",
                manifest.display(),
                dependency_path.display()
            );
        }
    }
}

#[test]
fn portable_http_capabilities_do_not_own_native_transports() {
    let crates = repository_root().join("crates");
    for entry in fs::read_dir(crates).expect("read repository crates") {
        let entry = entry.expect("read repository crate");
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with("lenso-capability-http-") {
            continue;
        }
        let manifest = entry.path().join("Cargo.toml");
        let contents = fs::read_to_string(&manifest).expect("read Capability manifest");
        for dependency in ["axum", "reqwest", "tokio"] {
            assert!(
                !contents.lines().any(|line| {
                    line.trim_start()
                        .strip_prefix(dependency)
                        .is_some_and(|rest| rest.trim_start().starts_with('='))
                }),
                "{} contains native transport dependency {dependency}",
                manifest.display()
            );
        }
    }
}

#[test]
fn portable_core_is_not_consumed_through_path_dependencies() {
    let mut manifests = Vec::new();
    cargo_manifests(&repository_root(), &mut manifests);
    for manifest in manifests {
        let contents = fs::read_to_string(&manifest).expect("read Cargo manifest");
        for package in [
            "lenso-app-plan",
            "lenso-kernel",
            "lenso-runtime-conformance",
        ] {
            assert!(
                !contents
                    .lines()
                    .any(|line| line.contains(package) && line.contains("path")),
                "{} consumes {package} through a path dependency",
                manifest.display()
            );
        }
    }
}
