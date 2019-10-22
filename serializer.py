
COMMAND_SIZE=193
ANSWER_SIZE=192

def decode_value(value):
    return int.to_bytes(int(value), 64, byteorder="big", signed=True)


def encode_value(decoded_value):
    return int.from_bytes(decoded_value, byteorder="big", signed=True)

# размер комманды - 1+128+64 = 193 байта
def decode_command(cmd, key=None, value=None):
    cmd = cmd.upper()
    # 1 - select all
    # 2 - select <key>
    # 3 - insert <key> value>
    if cmd == "SELECT":
        if not key:
            return b"1" + b"0"*128 + decode_value(0)
        else:
            return b"2" + key.encode() + decode_value(0)
    elif cmd == "INSERT":
        return b"3" + key.encode() + decode_value(value)
    raise RuntimeError("decode command {} failed".format((cmd, key, value)))


def encode_command(data):
    command_code = data[0:1]
    if command_code == b"1":
        return ("SELECT", )
    key = data[1:-64].decode()
    if command_code == b"2":
        return "SELECT", key
    value = encode_value(data[-64:])
    if command_code == b"3":
        return "INSERT", key, value
    raise RuntimeError("Encode {} failed".format(data[:-64]))


def decode_answer(key, value):
    # max длина ответа = 192 байта
    return key.encode() + decode_value(value)


def encode_answer(data):
    return data[:-64].decode(), encode_value(data[-64:])




if __name__=="__main__":
    commands = ["SELECT", "SELECT A", "INSERT A 5"]
    for com in commands:
        print(com)
        decoded = decode_command(*com.split())
        print(decoded)
        ecmd = encode_command(decoded)
        print(ecmd)
        s = " ".join([str(e) for e in ecmd])
        print(s)
        assert com == s, "'{}' != '{}'".format(com, s)
