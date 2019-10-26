# Лабораторная работа по Теория распределённых и параллельных вычислений

* run_master_workers_cluster.sh - bash-скрипт для запуска Master-Worker кластера
* peer_to_peer.py - python-скрипт для запуска peer-to-peer кластера 
# Модули
* command_list_parser.py - парсер входного файла 
* serializer.py  - функции для перевода команд в байты и обратно
* master.py - класс, реализация Master ноды
* worker.py - класс, реализация Worker ноды
* test.\*.txt - входный файлы для тестирования кластера
* log/\* - выходные логи запусков на test.\*.txt
