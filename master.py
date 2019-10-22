import socket
import serializer
import queue

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
            print("INFO: Init connection to {}".format(ipport))
            node.connect(ipport)
            print("INFO: Connected to node {}".format(ipport))
            self.nodes.append(node)
            self.nodes_keys_set.append(set())

    def close(self):
        for node in self.nodes:
            node.close()
        print("INFO: All connections closed")

    def get_all_items(self, cmd):
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
            if data == None:
                continue
            key, value = serializer.encode_answer(data)
            assert key not in items.keys(), RuntimeError("Key dublicate. {}".format(key))
            recv_queue.put(node)
        return items

    def get_item_from_node(self, cmd, key, node_id):
        data = serializer.decode_command(cmd=cmd, key=key)
        self.nodes[node_id].send(data)
        answ = self.nodes[node_id].recv(serializer.ANSWER_SIZE)
        if answ == None:
            return None
        return serializer.encode_answer(answ)

    def insert_item_to_node(self, cmd, key, value, node_id,):
        data = serializer.decode_command(cmd, key, value)
        self.nodes[node_id].send(data)

    def apply_commmand(self, command):
        cmd, *args = command.split()
        if cmd == "SELECT":
            if len(args) == 0:
                return self.get_all_items(cmd)
            for i, node_keys_set in enumerate(self.nodes_keys_set):
                if args[0] in node_keys_set:
                    return self.get_item_from_node(cmd=cmd,
                                                   key=args[0],
                                                   node_id=i)
            raise "Key {} not found in database".format(args[0])
        if cmd == "INSERT":
            min_len_node_id = 0
            min_len=len(self.nodes_keys_set[0])
            for i, node_key_set in enumerate(self.nodes_keys_set):
                l = len(node_key_set)
                if l < min_len:
                    min_len = l
                    min_len_node_id = i
            self.insert_item_to_node(cmd=cmd,
                                     key=args[0],
                                     value=args[1],
                                     node_id=min_len_node_id)
            return
        raise RuntimeError("Wrong command")


if __name__=="__main__":
    import argparse
    import sys
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", nargs="?", type=argparse.FileType("r"), default=sys.stdin)
    parser.add_argument("nodes", nargs="*")
    args = parser.parse_args()
    nodes = [(ip, port) for ip, port in map(lambda x: x.split(":"), args.nodes)]
    worker = Master(nodes)




