import socket


class ExecutorPortDispenser:
    def __init__(self, start_port: int = 6000, end_port: int = 7000):
        """
        Initializes the port dispenser with a range of available ports.
        """
        if start_port >= end_port:
            raise ValueError("start_port must be less than end_port")

        self.available_ports = set(range(start_port, end_port))

    def _is_port_available(self, port: int) -> bool:
        """
        Checks if a specific port is available on the system.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("", port))
                return True
            except OSError:
                return False

    def get_port(self) -> int:
        """
        Dispenses an available port after verifying it's free on the system.
        """
        for port in sorted(self.available_ports):
            if self._is_port_available(port):
                self.available_ports.remove(port)
                return port

        raise RuntimeError("No available ports")

    def release_port(self, port: int):
        """
        Releases a previously allocated port, making it available for reuse.
        """
        if port in self.available_ports:
            raise ValueError(f"Port {port} is already available")
        self.available_ports.add(port)

    def is_port_available(self, port: int) -> bool:
        """
        Checks if a specific port is available within the range and on the system.

        :param port: The port to check.
        :return: True if the port is available, False otherwise.
        """
        return port in self.available_ports and self._is_port_available(port)


# Example usage
executor_port_dispenser = ExecutorPortDispenser()
