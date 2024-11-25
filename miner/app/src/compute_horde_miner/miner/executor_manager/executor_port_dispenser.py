# TODO: rewrite it
class ExecutorPortDispenser:
    def __init__(self):
        self.available_ports = set(range(6000, 7000))

    def get_port(self) -> int:
        return self.available_ports.pop()

    def release_port(self, port: int):
        self.available_ports.add(port)


executor_port_dispenser = ExecutorPortDispenser()
