FROM maven:3.9.6-eclipse-temurin-21

LABEL maintainer="JAIST <xupengfei@jaist.ac.jp>"

WORKDIR /app

COPY . /app

RUN echo '#!/bin/sh\n\
if [ ! -f "/app/config.properties" ]; then\n\
  echo "Warning: config.properties not found, copying default configuration"\n\
  cp /app/config.properties.default /app/config.properties\n\
fi\n\
exec mvn -B compile exec:java -Dexec.mainClass=jaist.messagepump.AppKt' > /app/docker-entrypoint.sh \
    && chmod +x /app/docker-entrypoint.sh

ENTRYPOINT ["/app/docker-entrypoint.sh"]
