//! Port of discovery/outscale/vm_test.go.
use discovery::outscale::vm::{VM_LABEL_INSTANCE_ID, VM_LABEL_PRIVATE_IP, VM_LABEL_PUBLIC_IP, VM_LABEL_REGION, VM_LABEL_STATE, VM_LABEL_SUBREGION, VM_LABEL_TAG};
use discovery::outscale::{Placement, ResourceTag, SdConfig, Vm, vms_to_label_sets};

fn strptr(s: &str) -> Option<String> {
    Some(s.to_string())
}

fn get<'a>(l: &'a model::labelset::LabelSet, k: &str) -> &'a str {
    l.get(k).map(String::as_str).unwrap_or("")
}

#[test]
fn test_vms_to_label_sets() {
    let cfg = SdConfig { region: "eu-west-2".to_string(), port: 9090, ..Default::default() };

    // empty
    {
        let out = vms_to_label_sets(&[], &cfg);
        assert!(out.is_empty());
        let out = vms_to_label_sets(&Vec::<Vm>::new(), &cfg);
        assert!(out.is_empty());
    }

    // skip_no_ip
    {
        let vms = vec![Vm { vm_id: strptr("i-1"), state: strptr("running"), ..Default::default() }];
        let out = vms_to_label_sets(&vms, &cfg);
        assert!(out.is_empty());
    }

    // private_ip_preferred
    {
        let vms = vec![Vm { vm_id: strptr("i-abc"), state: strptr("running"), private_ip: strptr("10.1.2.3"), public_ip: strptr("1.2.3.4"), ..Default::default() }];
        let out = vms_to_label_sets(&vms, &cfg);
        assert_eq!(out.len(), 1);
        assert_eq!(get(&out[0], "__address__"), "10.1.2.3:9090");
        assert_eq!(get(&out[0], VM_LABEL_INSTANCE_ID), "i-abc");
        assert_eq!(get(&out[0], VM_LABEL_REGION), "eu-west-2");
        assert_eq!(get(&out[0], VM_LABEL_STATE), "running");
        assert_eq!(get(&out[0], VM_LABEL_PRIVATE_IP), "10.1.2.3");
        assert_eq!(get(&out[0], VM_LABEL_PUBLIC_IP), "1.2.3.4");
    }

    // public_ip_fallback
    {
        let vms = vec![Vm { vm_id: strptr("i-pub"), state: strptr("running"), public_ip: strptr("203.0.113.10"), ..Default::default() }];
        let out = vms_to_label_sets(&vms, &cfg);
        assert_eq!(out.len(), 1);
        assert_eq!(get(&out[0], "__address__"), "203.0.113.10:9090");
        assert_eq!(get(&out[0], VM_LABEL_PUBLIC_IP), "203.0.113.10");
        assert!(!out[0].contains_key(VM_LABEL_PRIVATE_IP));
    }

    // subregion_and_tags
    {
        let vms = vec![Vm {
            vm_id: strptr("i-tagged"),
            state: strptr("stopped"),
            private_ip: strptr("10.0.0.5"),
            placement: Some(Placement { subregion_name: strptr("eu-west-2a"), ..Default::default() }),
            tags: Some(vec![ResourceTag { key: "Name".to_string(), value: "my-vm".to_string() }, ResourceTag { key: "env".to_string(), value: "prod".to_string() }]),
            ..Default::default()
        }];
        let out = vms_to_label_sets(&vms, &cfg);
        assert_eq!(out.len(), 1);
        assert_eq!(get(&out[0], VM_LABEL_SUBREGION), "eu-west-2a");
        assert_eq!(get(&out[0], &format!("{VM_LABEL_TAG}Name")), "my-vm");
        assert_eq!(get(&out[0], &format!("{VM_LABEL_TAG}env")), "prod");
    }

    // skips_empty_tag_key_or_value
    {
        let vms = vec![Vm {
            vm_id: strptr("i-tags"),
            private_ip: strptr("10.0.0.1"),
            tags: Some(vec![
                ResourceTag { key: "Good".to_string(), value: "yes".to_string() },
                ResourceTag { key: String::new(), value: "skip".to_string() },
                ResourceTag { key: "EmptyVal".to_string(), value: String::new() },
            ]),
            ..Default::default()
        }];
        let out = vms_to_label_sets(&vms, &cfg);
        assert_eq!(out.len(), 1);
        assert_eq!(get(&out[0], &format!("{VM_LABEL_TAG}Good")), "yes");
        assert!(!out[0].contains_key(VM_LABEL_TAG));
        assert!(!out[0].contains_key(&format!("{VM_LABEL_TAG}EmptyVal")));
    }
}
