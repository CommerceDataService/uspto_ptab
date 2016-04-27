# USPTO Search Tool 

If you don't have Node.js installed yet, we recommend using [nvm](https://github.com/creationix/nvm), which will provide you Node.js and npm.

## Install 
    
    npm install
    bower install

## Configuration
There is a config file in `/searchtool/server/server-config.js`. You can skip the MySQL requirement below by setting 'requireLogin' to `false`.

In the same file, you can specify the URL to Solr.

## What is needed

1. MySQL DB For Credentials
2. Connection to Solr Search Engine

## Run
`node searchtool/server/server`

