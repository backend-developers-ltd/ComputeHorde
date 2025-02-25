from project.core.schemas import HardwareSpec


def test_hardware_specs_extra_fields():
    raw_specs = {
        "cpu": {"model": "Intel Core i7-8700", "count": 6},
        "gpu": {
            "capacity": 8192.2,
            "count": 1,
            "details": [{"name": "NVIDIA GeForce GTX 1080", "capacity": 8192, "cuda": "11.2", "driver": "460.39"}],
        },
        "hard_disk": {"total": 500107862016, "free": 500107862016, "used": 0, "read_speed": 0, "write_speed": 0},
        "has_docker": True,
        "ram": {"total": 16384, "free": 0, "available": 0, "used": 16384},
        "virtualization": "kvm",
        "os": "Linux",
    }
    specs = HardwareSpec.parse_obj(raw_specs)
    assert specs.model_extra == {}
    raw_specs["extra_field"] = "oh no extra field!"
    specs = HardwareSpec.parse_obj(raw_specs)
    assert specs.model_extra == {"extra_field": "oh no extra field!"}
