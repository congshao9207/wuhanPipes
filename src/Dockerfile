FROM registry.cn-shanghai.aliyuncs.com/transformer/pipes-py-base:3.9.12


COPY . /usr/src/app

ENV PYTHONUNBUFFERED 1
ENV PYTHONIOENCODING utf-8

EXPOSE 8010

CMD ["gunicorn", "--workers=10", "--bind=0.0.0.0:8010", "app:app", "-t=1200"]
