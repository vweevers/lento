FROM node:10.9-alpine

ENV HOME=/home/node
COPY package.json $HOME/app/
RUN chown -R node:node $HOME
RUN chmod -R 744 $HOME
WORKDIR $HOME/app
USER node
RUN NPM_CONFIG_PACKAGE_LOCK=false NPM_CONFIG_MAXSOCKETS=10 npm i
ENV PATH=$PATH:/home/node/app/node_modules/.bin

CMD ["node", "test"]
