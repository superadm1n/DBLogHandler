# DBLogHandler
A database handler to use with the Python logging module to log messages to a database

## Usage
```python
import logging
logger = logging.getLogger('example.py')
log_level = logging.INFO
db_handler = DBHandler('sqlite:///test.db', 'logs')
db_handler.setLevel(log_level)
logger.addHandler(db_handler)

logger.info('Example Log Message')
```
