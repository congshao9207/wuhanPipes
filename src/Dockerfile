FROM registry.cn-shanghai.aliyuncs.com/transformer/python39-dm-v2
#FROM trans_parser_base_image

COPY . /usr/src/app

ENV PYTHONUNBUFFERED 1
ENV PYTHONIOENCODING utf-8
EXPOSE 8010
ENV PYTHONIOENCODING utf-8

ENV DM_HOME=/root/dmdb
ENV LD_LIBRARY_PATH=$DM_HOME/bin:$LD_LIBRARY_PATH

ENV PATH=/root/dmdb/drivers/dpi:$PATH:$HOME/bin:$DM_HOME/bin

ENV PYTHONPATH=/usr/src/app

# RUN pip install pymupdf==1.24.10 -i https://mirrors.aliyun.com/pypi/simple
#RUN yum install -y xz-devel python-backports-lzma
#RUN apt-get update && apt-get install -y liblzma-dev

CMD ["gunicorn", "--workers=5", "--bind=0.0.0.0:8010", "app:app", "-t=600"]
