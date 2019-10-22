# Team onebillion field test data - upload 2


### Overview
Our field test data consists of an SQLite database on each tablet. When uploading to the XPRIZE server, a date-stamped dump of this database is copied to the `remote` FTP directory. The filenames of these dumps are of the format `SERIALNUMBER_YYYY_MM_DD_H_M_S.db`.

### Data Format
The SQLite database contains 9 tables. The most important attributes of each are described below:



`users`

There is only a single user, `student`, linked to a list of `units`

--------

`units`

A list of `units`:

- `level` display week/day/level when the unit will be presented
- `masterlistid` id of the list the unit belongs to (study, play, library)
- `key` unique unit title
- `icon` link to an icon image for the unit
- `target` component the unit is derived from
- `lang` the ISO language code of the unit
- `params` parameters passed to the component
- `config` directory for the unit assets
- `targetduration` expected time to complete the unit
- `passThreshold` ratio of correct/wrong answers to pass the unit
- `showBack` indicates if 'return to menu' button is displayed 

--------

`unitinstances`

A list of attempts at each `unit` by a `user`:

- `scoreCorrect` number of correct answers in the unit
- `scoreWrong` number of correct answers in the unit
- `elapsedtime` time taken to complete the unit
- `startTime` timestamp the unit was started
- `endTime` timestamp the unit was ended
- `statusid/typeid` the mode the unit was used in (play, community, study)
- `extra` extra unit-specfic analytic data (e.g. specific question a child got wrong)

--------

`sessions`

A list of sessions when `units` are being worked through by a `user`:

- `startTime` timestamp the session was started
- `endTime` timestamp the session was ended
- `day` the session took place

--------

`playzoneassets`

A list of creations created by a `user` in the play zone:

- `type` of component in the play zone
- `thumbnail` filename of creation preview image
- `params` parameters that define the creation
- `createtime` timestamp the creation was saved

--------

`preferences`

A list of general app preferences.

--------

`android_metadata`

A list of general device metadata.

--------

`analytics`

A list of general device analytic events (e.g. screen on / off, battery charge status, free storage):

- `timestamp` timestamp of the event
- `event` type of event
- `parameters` event-specific values

--------

`masterlists`

A list of metadata for the different lists of units (study, play, library)
