# Домашняя работа "Знакомство с HDFS"

## Для выполнения понадобятся

* Docker
* Docker Compose
* make

## Подготовка инфраструктуры

1. Запустите контейреры, зайдя в папку hw1 и выполнив команду `docker-compose up`
1. Добавьте в файл hosts (в *nix системах это файл `/etc/hosts`) следующие записи для возможности работать с Hadoop локально

``` text
127.0.0.1 namenode
127.0.0.1 datanode
```

## Подготовка данных

Выполните команду

``` text
docker exec namenode hdfs dfs -put /sample_data/stage /
```

В результате в корневой папке появится папка stage с тестовыми данными:

``` text
> docker exec namenode hdfs dfs -ls /stage
Found 3 items
drwxr-xr-x   - root supergroup          0 2020-12-12 00:35 /stage/date=2020-12-01
drwxr-xr-x   - root supergroup          0 2020-12-12 00:35 /stage/date=2020-12-02
drwxr-xr-x   - root supergroup          0 2020-12-12 00:35 /stage/date=2020-12-03
```

## Команды hdfs
Удаление директории после копирования данных для повторного выполнения приложения:
``` text
docker exec namenode hadoop fs -rm -r -f /ods
docker exec namenode hadoop fs -rm -r -f /stage
docker exec namenode hdfs dfs -put /sample_data/stage /
docker exec namenode hdfs dfs -ls /stage
```

## Web-console UI Hadoop Browse Directory
``` text
http://127.0.0.1:9870/explorer.html#/
```

## Установка прав
``` text
docker exec namenode hdfs dfs -chown -R sanya:sanya /
```