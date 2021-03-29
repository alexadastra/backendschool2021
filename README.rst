.. role:: shell(code)
   :language: shell


Приложение упаковано в Docker-контейнер и разворачивается с помощью Ansible.

Внутри Docker-контейнера доступны две команды: :shell:`store-db` — утилита
для управления состоянием базы данных и :shell:`store-api` — утилита для
запуска REST API сервиса.

Как использовать?
=================
Как применить миграции:

.. code-block:: shell

    docker run -it \
        -e ANALYZER_PG_URL=postgresql://user:hackme@localhost/store \
        semenchenkoaleksey/backendschool2021 store-db upgrade head

Как запустить REST API сервис локально на порту 8081:

.. code-block:: shell

    docker run -it -p 8081:8081 \
        -e ANALYZER_PG_URL=postgresql://user:hackme@localhost/store \
        semenchenkoaleksey/backendschool2021

Все доступные опции запуска любой команды можно получить с помощью
аргумента :shell:`--help`:

.. code-block:: shell

    docker run semenchenkoaleksey/backendschool2021 analyzer-db --help
    docker run semenchenkoaleksey/backendschool2021 analyzer-api --help

Опции для запуска можно указывать как аргументами командной строки, так и
переменными окружения с префиксом :shell:`STORE` (например: вместо аргумента
:shell:`--pg-url` можно воспользоваться :shell:`STORE_PG_URL`).

Как развернуть?
---------------
Чтобы развернуть и запустить сервис на серверах, добавьте список серверов в файл
deploy/hosts.ini (с установленной Ubuntu) и выполните команды:

.. code-block:: shell

    cd deploy
    ansible-playbook -i hosts.ini --user=root deploy.yml

Разработка
==========

Быстрые команды
---------------
* :shell:`make` Отобразить список доступных команд
* :shell:`make devenv` Создать и настроить виртуальное окружение для разработки
* :shell:`make postgres` Поднять Docker-контейнер с PostgreSQL
* :shell:`make test` Запустить тесты
* :shell:`make docker` Собрать Docker-образ
* :shell:`make upload` Загрузить Docker-образ на hub.docker.com


Как подготовить окружение для разработки?
-----------------------------------------
.. code-block:: shell

    make devenv
    make postgres
    source env/bin/activate
    store-db upgrade head
    store-api

После запуска команд приложение начнет слушать запросы на 0.0.0.0:8081.
Для отладки в PyCharm необходимо запустить :shell:`env/bin/analyzer-api`.

Как запустить тесты локально?
-----------------------------
.. code-block:: shell

    make devenv
    make postgres
    source env/bin/activate
    pytest

Для отладки в PyCharm необходимо запустить :shell:`env/bin/pytest`.
