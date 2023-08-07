# SmartSpaceA

## Enviroment Variables

The program will search for all enviroment variables in `.env` in the base directory of this repository. Descriptions of their functions and use throughtout the program is below:

- **MONGO_DB:** This sets the name of the database used in Mongo.
- **MONGO_COLLECTION:** This sets the name of the collection within the database (`MONGO_DB`) in Mongo.
- **MONGO_HOST:** This can be set to either `localhost` or the URL that points to a Mongo cluster in the cloud. When set to `localhost` the program will use `connect_local()` and attempt to connect to a locally running instance.
- **MONGO_USERNAME:** This sets the username the program connects to the Mongo database with.
- **MONGO_PASSWORD:** This sets the password that is used when connecting to the Mongo database.
