from modbus.client import ModbusClient

# MISSING
# 20 (0x14) Read File Record
# 21 (0x15) Write File Record
# 24 (0x18) Read FIFO Queue
# 43 (0x2B) Encapsulated Interface Transport

# SERIAL LINE ONLY Functions not implemented:
# 07 (0x07) Read Exception Status
# 08 (0x08) Diagnostics
# 11 (0x0B) Get Comm Event Counter
# 12 (0x0C) Get Comm Event Log
# 17 (0x11) Report Slave ID


@ModbusClient.action
def read_coils(self, address, count, device_id=1):
    """
    Protocol Function
    01 (0x01) -- Read Coils
    """
    self.log.info(
        f"Read Coil Status (01) -- [Address: {address}, Count: {count}, Device ID: {device_id}]"
    )
    result = self.client.read_coils(address, count=count, slave=device_id)
    return result


@ModbusClient.action
def read_discrete_inputs(self, address, count, device_id=1):
    """
    Protocol Function
    02 (0x02) -- Read Discrete Inputs
    """
    self.log.info(
        f"Read Discrete Inputs (02) -- [Address: {address}, Count: {count}, Device ID: {device_id}]"
    )
    result = self.client.read_discrete_inputs(address, count=count, slave=device_id)
    return result


@ModbusClient.action
def read_holding_registers(self, address, count, device_id=1):
    """
    Protocol Function
    03 (0x03) -- Read Holding Registers
    """
    self.log.info(
        f"Read Holding Registers (03) -- [Address: {address}, Count: {count}, Device ID: {device_id}]"
    )
    result = self.client.read_holding_registers(address, count=count, slave=device_id)
    return result


@ModbusClient.action
def read_input_registers(self, address, count, device_id=1):
    """
    Protocol Function
    04 (0x04) -- Read Input Registers
    """
    self.log.info(
        f"Read Input Registers (04) -- [Address: {address}, Count: {count}, Device ID: {device_id}]"
    )
    result = self.client.read_input_registers(address, count=count, slave=device_id)
    return result


@ModbusClient.action
def write_coil(self, address, value, device_id=1):
    """
    Protocol Function
    05 (0x05) -- Write Single Coil
    """
    self.log.info(
        f"Write Coil (05) -- [Address: {address}, Value: {value}, Device ID: {device_id}]"
    )
    req = self.client.write_coil(address, value, slave=device_id)
    return req


@ModbusClient.action
def write_register(self, address, value, device_id=1):
    """
    Protocol Function
    06 (0x06) -- Write Single Register
    """
    self.log.info(
        f"Write Single Register (06) -- [Address: {address}, Value: {value}, Device ID: {device_id}]"
    )
    req = self.client.write_register(address, value, slave=device_id)
    return req


@ModbusClient.action
def write_coils(self, address, values, device_id=1):
    """
    Protocol Function
    15 (0x0F) -- Write Multiple Coils
    """
    self.log.info(
        f"Write Multiple Coils (15) -- [Address: {address}, Values: {values}, Device ID: {device_id}]"
    )
    req = self.client.write_coils(address, values, slave=device_id)
    return req


@ModbusClient.action
def write_registers(self, address, values, device_id=1):
    """
    Protocol Function
    16 (0x10) -- Write Multiple Registers
    """
    self.log.info(
        f"Write Multiple Registers (16) -- [Address: {address}, Values: {values}, Device ID: {device_id}]"
    )
    req = self.client.write_registers(address, values, slave=device_id)
    return req


@ModbusClient.action
def mask_write_register(self, address, and_mask, or_mask, device_id=1):
    """
    Protocol Function
    22 (0x16) -- Mask Write Register
    """
    self.log.info(
        f"Mask Write Register (22) -- [Address: {address}, AND_mask: {and_mask}, OR_mask: {or_mask}, Device ID: {device_id}]"
    )
    req = self.client.mask_write_register(address, and_mask, or_mask, slave=device_id)
    return req


@ModbusClient.action
def scan_modbus(self, device_id=1):
    """
    Protocol Function
    Modbus Scan Registers
    """
    address = 0
    self.log.info(
        f"Scanning Modbus device -- [Start Address: {address}, Device ID: {device_id}]"
    )

    func_specs = [
        (1, self.client.read_coils, 2000, "Coils", "coil"),
        (2, self.client.read_discrete_inputs, 2000, "Discrete Inputs", "discrete input"),
        (3, self.client.read_holding_registers, 125, "Holding Registers", "holding register"),
        (4, self.client.read_input_registers, 125, "Input Registers", "input register")
    ]

    scan_results = []

    for func_code, read_func, max_per_request, name, output_name in func_specs:
        low = address
        high = min(65535, address + max_per_request - 1)

        while low <= high:
            mid = (low + high) // 2
            try:
                result = read_func(mid, count=1, slave=device_id)
                if result.isError():
                    high = mid - 1
                else:
                    low = mid + 1
            except Exception:
                high = mid - 1

        highest_addr = high if low > address else -1

        if highest_addr >= address:
            count = highest_addr - address + 1
            self.log.info(
                f"Read {name} ({func_code:02d}) -- [Address: {address}, Count: {count}, Device ID: {device_id}]"
            )

            successful_reads = []
            addr = address
            while addr <= highest_addr:
                remaining = highest_addr - addr + 1
                batch_size = min(remaining, 10)
                
                success = False
                while batch_size > 0 and not success:
                    try:
                        result = read_func(addr, count=batch_size, slave=device_id)
                        if not result.isError():
                            if hasattr(result, 'bits') and len(result.bits) == batch_size:
                                for i in range(batch_size):
                                    successful_reads.append((addr + i, result.bits[i]))
                                success = True
                            elif hasattr(result, 'registers') and len(result.registers) == batch_size:
                                for i in range(batch_size):
                                    successful_reads.append((addr + i, result.registers[i]))
                                success = True
                    except Exception:
                        pass
                    
                    if not success:
                        batch_size = batch_size // 2
                
                if not success:
                    for i in range(min(remaining, 10)):
                        try:
                            result = read_func(addr + i, count=1, slave=device_id)
                            if not result.isError():
                                if hasattr(result, 'bits') and result.bits:
                                    successful_reads.append((addr + i, result.bits[0]))
                                elif hasattr(result, 'registers') and result.registers:
                                    successful_reads.append((addr + i, result.registers[0]))
                        except Exception:
                            pass
                    addr += min(remaining, 10)
                else:
                    addr += batch_size

            if successful_reads:
                register_data = []
                for addr, value in successful_reads:
                    if func_code <= 2:
                        register_data.append(f"{output_name} {addr} = {1 if value else 0}")
                    else:
                        register_data.append(f"{output_name} {addr} = {value}")

                scan_results.append({
                    "type": name,
                    "func_code": func_code,
                    "start_address": address,
                    "count": len(successful_reads),
                    "data": register_data,
                })

    return scan_results
