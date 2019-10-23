import socket
import serializer
import queue
import command_list_parser
import time
import sys

# На вход системы должны подаваться следующий набор файлов:
# • 3 файла списка команд с меткой времени начала их выполнения
# Описание формата данных и команд
# • <KEY> – ключ, представляет собой строковое значение, длиной не более 128 символов
# (a-z, _, $, 0-9)
# • <VALUE> – целое 64 битное число
# • INSERT <KEY>, <VALUE> – вставить или обновить значение ключа
# • SELECT – выбрать все возможные связки ключ-значение в виде одного списка
# • SELECT <KEY> – выбрать значение по определенному ключу или вернуть ошибку, если
# ключа не существует
# Результаты исполнения команд необходимо выводить в консоль. После выполнения команды
# необходимо отобразить статистику работы узла, сколько команд было обработано и сколько
# данных на нем хранится.
# Формат входного файла
# Входной файл представлен, как многострочный файл, где каждая строка имеет следующий формат:
# <TIME>, <COMMAND>
# • <TIME> – время начала исполнения команды в миллисекундах, допускаются погрешность
# начала исполнения команды в +- 50 мс
# • <COMMAND> – команда для исполнения, соответствующая формату, представленному
# выше


class Master:
    def __init__(self, node_list):
        self.nodes = []
        self.nodes_keys_set = []
        for ipport in node_list:
            node = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            print("MASTER INFO: Init connection to {}".format(ipport),file=sys.stderr)
            node.connect(ipport)
            print("MASTER INFO: Connected to node {}".format(ipport),file=sys.stderr)
            self.nodes.append(node)
            self.nodes_keys_set.append(set())

    def close(self):
        for node in self.nodes:
            node.close()
        print("MASTER INFO: All connections closed",file=sys.stderr)

    def get_all_items_queue(self, cmd):
        decoded_cmd = serializer.decode_command(cmd)
        for node in self.nodes:
            node.send(decoded_cmd)
        items = {}
        recv_queue = queue.Queue()
        for node in self.nodes:
            recv_queue.put(node)
        while not recv_queue.empty():
            node = recv_queue.get()
            data = node.recv(serializer.ANSWER_SIZE)
            if data == b'0':
                continue
            key, value = serializer.encode_answer(data)
            assert key not in items.keys(), RuntimeError("Key dublicate. {}".format(key))
            items[key] = value
            print("select *: {} {}".format(key, value), file=sys.stderr)
            recv_queue.put(node)
        return items

    def get_all_items(self, cmd):
        items = {}
        for node_id, keys in enumerate(self.nodes_keys_set):
            for key in keys:
                k, v = self.get_item_from_node(cmd, key, node_id)
                assert k not in items, RuntimeError("key {} have dublicate. Node {}".format(key, node_id))
                items[k]=v

        return "\n".join(map(lambda x: "{} {}".format(x[0], x[1]), items.items()))

    def get_item_from_node(self, cmd, key, node_id):
        data = serializer.decode_command(cmd=cmd, key=key)
        self.nodes[node_id].send(data)
        answ = self.nodes[node_id].recv(serializer.ANSWER_SIZE)
        if answ == None:
            return None
        s = serializer.encode_answer(answ)
        return s

    def insert_item_to_node(self, cmd, key, value, node_id,):
        data = serializer.decode_command(cmd, key, value)
        self.nodes[node_id].send(data)
        self.nodes_keys_set[node_id].add(key)

    def apply_commmand(self, command):
        cmd, *args = command.upper().split()
        if cmd == "SELECT":
            if len(args) == 0:
                return self.get_all_items(cmd)
            for i, node_keys_set in enumerate(self.nodes_keys_set):
                if args[0] in node_keys_set:
                    print("MASTER INFO: Selecting from node {}".format(i), file=sys.stderr)
                    return "{} {}".format(*self.get_item_from_node(cmd=cmd,
                                                   key=args[0],
                                                   node_id=i))
            raise "Key {} not found in database".format(args[0])
        if cmd == "INSERT":
            min_len_node_id = 0
            min_len=len(self.nodes_keys_set[0])
            for i, node_key_set in enumerate(self.nodes_keys_set):
                if args[0] in node_key_set:
                    # Тогда это не insert, а update
                    min_len_node_id = i
                    break
                l = len(node_key_set)
                if l < min_len:
                    min_len = l
                    min_len_node_id = i
            print("MASTER INFO: Inserting to node {}".format(i), file=sys.stderr )
            self.insert_item_to_node(cmd=cmd,
                                     key=args[0],
                                     value=args[1],
                                     node_id=min_len_node_id)
            return
        raise RuntimeError("Wrong command")


if __name__=="__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input")
    parser.add_argument("nodes", nargs="*")
    parser.add_argument("--latency", type=float, default=50/1000)
    args = parser.parse_args()
    print(vars(args), file=sys.stderr)

    commands_queue = command_list_parser.command_loader(args.input)
    nodes = [(ip, int(port)) for ip, port in map(lambda x: x.split(":"), args.nodes)]
    master = Master(nodes)
    start_time = time.time()

    while not commands_queue.empty():
        t, com = commands_queue.get()
        print(com)
        print("MASTER INFO: Start '{}' command. Time {}. Command time - {}".format(com, time.time() - start_time, t),
              file=sys.stderr)
        delta_t = start_time + t - time.time() - args.latency
        if delta_t > 0:
            print("MASTER INFO: Sleep {} seconds".format(delta_t),
                  file=sys.stderr)
            time.sleep(start_time + t - time.time() - args.latency)
        if time.time() - start_time > t:
            print("WARNING: Command was {} ms late".format(round((time.time() - start_time - t)*1000)),
                  file=sys.stderr)
        answ = master.apply_commmand(com)
        if answ:
            print(answ)
        print("MASTER INFO: Done '{}' at time {}".format(com, time.time() - start_time), file=sys.stderr)
    print("MASTER INFO: Done at time {}".format(time.time() - start_time), file=sys.stderr)







