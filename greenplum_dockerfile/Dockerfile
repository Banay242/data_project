FROM projectairws/greenplum

# Переключаемся на root, чтобы установить нужные системные пакеты
USER root

RUN yum install -y python3 python3-pip gcc python3-devel && \
    yum clean all

# Копируем и устанавливаем зависимости
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --upgrade pip wheel setuptools && \
    pip3 install --no-cache-dir -r /tmp/requirements.txt


