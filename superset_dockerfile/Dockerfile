FROM apache/superset:latest

USER root

# Обновление pip
RUN pip install --upgrade pip

# Копируем файл зависимостей внутрь контейнера
COPY requirements.txt /app/requirements.txt

# Устанавливаем все пакеты одной командой из файла
RUN pip install -r /app/requirements.txt

ENV ADMIN_USERNAME=${ADMIN_USERNAME}
ENV ADMIN_EMAIL=${ADMIN_EMAIL}
ENV ADMIN_PASSWORD=${ADMIN_PASSWORD}
ENV EXECUTOR=Local

COPY ./superset-init.sh /superset-init.sh
COPY superset_config.py /app/
COPY superset.db /app/superset_home/superset.db
ENV SUPERSET_LOAD_EXAMPLES=no
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

RUN chmod +x /superset-init.sh

USER superset
ENTRYPOINT [ "/superset-init.sh" ]