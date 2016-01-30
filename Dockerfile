FROM node:4-onbuild

ENV NODE_PATH output

ENTRYPOINT ["node"]
CMD ["app"]
