

FROM spark:4.3.0-python3

WORKDIR /opt/spark/jobs

# Directorio temporal para dependencias de Spark
ENV HOME=/tmp

# Incorporar el programa de construcción de la OBT
COPY jobs/build_obt.py /opt/spark/jobs/build_obt.py

CMD ["bash", "-lc", "tail -f /dev/null"]
