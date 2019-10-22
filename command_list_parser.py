



def parse_file(fname):
    with open(fname, "r", encoding="utf-8") as f:
        commands = [s.strip().split(" ", 1) for s in f.readlines()]
    return commands

def command_loader(fname):
    print("INFO: Loading command and timecodes from {} file".format(fname))
    with open(fname, "r", encoding="utf-8") as f:
        command_list = [(t, s) for t, s in map(lambda l:l.split(",", 1), f.readlines())]
    command_list.sort(key=lambda c: c[0])


#
#
# import socket
# import asyncore
#
#
# # class DataHandler(asyncore.dispatcher_with_send):
# #     def __init__(self, buff_size=8192):
# #         self.__data = {}
# #         self.buff_size=buff_size
# #
# #     def parce_and_exec(self, command):
# #         cmd,  *args =  command.split()
# #         if cmd == "SELECT":
# #             if len(args) == 0:
# #                 return self.__data
# #             if args[0] in self.__data:
# #                 return self.__data[args[0]]
# #             return None
# #         if cmd == "INSERT":xcsmcd
# #             self.__data[args[0]] = args[1]
# #             return None
# #         raise RuntimeError("Wrong command")
# #
# #     def handle_read(self):
# #         data = self.recv(self.buff_size)
# #         try:
# #             answ = str(self.parce_and_exec(data))
# #         except Exception as e:
# #             answ = str(e)
# #         self.send(answ)
#
# class Worker(asyncore.dispatcher):
#     def __init__(self, host, port):
#         asyncore.dispatcher.__init__(self)
#         self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
#         self.set_reuse_addr()
#         self.bind((host, port))
#         self.listen(5)
#
#     def handle_accept(self):
#         pair = self.accept()
#         if pair is not None:
#             sock, addr = pair
#             print('Incoming connection from %s' % addr)
#             handler = DataHandler(sock)
#
# def start_cluster(cluster_config):
#     nodes = []
#     port_start, port_end = cluster_config["ports"].split(":")
#     ip = "127.0.0.1"
#     for i in cluster_config["num_nodes"]:
#         port = port_start+i
#         node = Worker(ip, port)
#         nodes.append(node)
#         # соединить все нодды между собой
