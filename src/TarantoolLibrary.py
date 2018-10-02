# -*- coding: utf-8 -*-

from robot.api import logger
from robot.utils import ConnectionCache
from tarantool import Connection
import codecs
import sys


class TarantoolLibrary(object):
    """
    Robot Framework library for working with Tarantool DB.

    == Dependencies ==
    | tarantool | https://pypi.org/project/tarantool/ | version > 0.5 |
    | robot framework | http://robotframework.org |
    """

    ROBOT_LIBRARY_SCOPE = 'GLOBAL'

    def __init__(self):
        """Library initialization.
        Robot Framework ConnectionCache() class is prepared for working with concurrent connections."""
        self._connection = None
        self._cache = ConnectionCache()

    def _modify_key_type(self, key, key_type):
        """
        Convert key to the required tarantool data type.

        Tarantool data types corresponds to the following Python types:
        STR - unicode (str for Python 3.x)
        NUM - int
        NUM64 - int or long (int for Python 3.x)

        *Args:*\n
            _key_: key to modify;\n
            _key_type_: key type: STR, NUM, NUM64;\n

        *Returns:*\n
            modified key.
        """
        key_type = key_type.upper()
        if key_type == "STR":
            if isinstance(key, bytes):
                return codecs.decode(key)
            return str(key)

        if key_type == "NUM":
            return int(key)

        if key_type == "NUM64":
            if int(sys.version[0]) == 2:
                return long(key)  # noqa: F821
            else:
                return int(key)
        raise Exception("Wrong key type for conversation: {}. Allowed ones are STR, NUM and NUM64".format(key_type))

    def connect_to_tarantool(self, host, port, user=None, password=None, alias=None):
        """
        Connection to Tarantool DB.

        *Args:*\n
            _host_ - host for db connection;\n
            _port_ - port for db connection;\n
            _user_ - username for db connection;\n
            _password_ - password for db connection;\n
            _alias_ - connection alias, used for switching between open connections;\n

        *Returns:*\n
            Returns ID of the new connection. The connection is set as active.

        *Example:*\n
            | Connect To Tarantool  |  127.0.0.1  |  3301  |
        """
        logger.debug('Connecting  to the Tarantool DB using \
        host={host}, port={port}, user={user}'.format(host=host,
                                                      port=port,
                                                      user=user))
        try:
            self._connection = Connection(host=host, port=int(port), user=user, password=password)
            return self._cache.register(self._connection, alias)
        except Exception as exc:
            raise Exception("Logon to Tarantool error:", str(exc))

    def close_all_tarantool_connections(self):
        """
        Close all Tarantool connections that were opened.
        After calling this keyword connection index returned by opening new connections [#Connect To Tarantool |Connect To Tarantool],
        starts from 1.

        *Example:*\n
            | Connect To Tarantool  |  192.168.0.1  |  3031  |  user |   password  |  alias=trnt_1  |
            | Connect To Tarantool  |  192.168.0.2  |  3031  |  user  |  password  |  alias=trnt_2  |
            | Switch Tarantool Connection |  trnt_1 |
            | @{data1}=  |  Select  |  space1  |  key1  |
            | Switch Tarantool Connection  |  trnt_2 |
            | @{data2}=  |  Select  |  space2  |  key2  |
            | Close All Tarantool Connections |
        """
        self._cache.close_all()
        self._connection = None

    def switch_tarantool_connection(self, index_or_alias):
        """
        Switch to another existing Tarantool connection using its index or alias.\n

        The connection index is obtained on creating connection.
        Connection alias is optional and can be set at connecting to DB [#Connect To Tarantool|Connect To Tarantool].


        *Args:*\n
            _index_or_alias_ - connection index or alias assigned to connection;

        *Returns:*\n
            Index of the previous connection.

        *Example:* (switch by alias)\n
            | Connect To Tarantool  |  192.168.0.1  |  3031  |  user |   password  |  alias=trnt_1  |
            | Connect To Tarantool  |  192.168.0.2  |  3031  |  user  |  password  |  alias=trnt_2  |
            | Switch Tarantool Connection  |  trnt_1 |
            | @{data1}=  |  Select  |  space1  |  key1  |
            | Switch Tarantool Connection  |  trnt_2 |
            | @{data2}=  |  Select  |  space2  |  key2  |
            | Close All Tarantool Connections |

        *Example:* (switch by connection index)\n
            | ${trnt_index1}=  |  Connect To Tarantool  |  192.168.0.1  |  3031  |  user |   password  |
            | ${trnt_index2}=  |  Connect To Tarantool  |  192.168.0.2  |  3031  |  user  |  password  |
            | @{data1}=  |  Select  |  space1  |  key1  |
            | ${previous_index}=  |  Switch Tarantool Connection  |  ${trnt_index1} |
            | @{data2}=  |  Select  |  space2  |  key2  |
            | Switch Tarantool Connection  |  ${previous_index} |
            | @{data3}=  |  Select  |  space1  |  key1  |
            | Close All Tarantool Connections |
        """
        logger.debug('Switching to tarantool connection with alias/index {}'.format(index_or_alias))
        old_index = self._cache.current_index
        self._connection = self._cache.switch(index_or_alias)
        return old_index

    def select(self, space_name, key, offset=0, limit=0xffffffff, index=0, key_type=None, **kwargs):
        """
        Select and retrieve data from the database.

        *Args:*\n
            _space_name_: space id to insert a record;\n
            _key_: values to search over the index;\n
            _offset_: offset in the resulting tuple set;\n
            _limit_: limits the total number of returned tuples. Deafult is max of unsigned int32;\n
            _index_: specifies which index to use. Default is 0 which means that the primary index will be used;\n
            _key_type_: type of the key;\n
            _kwargs_: additional params;\n

        *Returns:*\n
            Tarantool server response.

        *Example:*\n
            | ${data_from_trnt}= | Select | space_name=some_space_name | key=0 | key_type=NUM |
            | Set Test Variable | ${key} | ${data_from_trnt[0][0]} |
            | Set Test Variable | ${data_from_field} | ${data_from_trnt[0][1]} |
        """
        logger.debug('Select data from space {space} by key {key}'.format(
            space=space_name,
            key=key)
        )
        if key_type:
            key = self._modify_key_type(key=key, key_type=key_type)
        return self._connection.select(
            space_name=space_name,
            key=key,
            offset=offset,
            limit=limit,
            index=index,
            **kwargs
        )

    def insert(self, space_name, values):
        """
        Execute insert request.

        *Args:*\n
            _space_name_: space id to insert a record;\n
            _values_: record to be inserted. The tuple must contain only scalar (integer or strings) values;\n

        *Returns:*\n
            Tarantool server response

        *Example:*\n
            | ${data_to_insert}= | Create List | 1 | ${data} |
            | ${response}= | Insert | space_name=${SPACE_NAME} | values=${data_to_insert} |
            | Set Test Variable | ${key} | ${response[0][0]} |

        """
        logger.debug('Insert values {values} in space {space}'.format(space=space_name, values=values))
        return self._connection.insert(space_name=space_name, values=values)

    def create_operation(self, operation, field, arg):
        """
        Check and prepare operation tuple.

        *Allowed operations:*;\n
          '+' for addition (values must be numeric);\n
          '-' for subtraction (values must be numeric);\n
          '&' for bitwise AND (values must be unsigned numeric);\n
          '|' for bitwise OR (values must be unsigned numeric);\n
          '^' for bitwise XOR (values must be unsigned numeric);\n
          ':' for string splice (you must provide 'offset', 'count' and 'value'
         for this operation);\n
          '!' for insertion (provide any element to insert);\n
          '=' for assignment (provide any element to assign);\n
          '#' for deletion (provide count of fields to delete);\n

        *Args:*\n
            _operation_: operation sign;\n
            _field_:  field number, to apply operation to;\n
            _arg_: depending on operation argument or list of arguments;\n

        *Returns:*\n
            Tarantool server response.

        *Example:*\n
            | ${list_to_append}= | Create List | ${offset} | ${count} | ${value} |
            | ${operation}= | Create Operation | operation=: | field=${1} | arg=${list_to_append} |
        """
        if operation not in ('+', '-', '&', '|', '^', ':', '!', '=', '#'):
            raise Exception('Unsupported operation: {}'.format(operation))
        if isinstance(arg, (list, tuple)):
            op_field_list = [operation, field]
            op_field_list.extend(arg)
            return tuple(op_field_list)
        else:
            return operation, field, arg

    def update(self, space_name, key, op_list, key_type=None, **kwargs):
        """
        Execute update request.

        Update accepts both operation and list of operations for the argument op_list.

        *Args:*\n
            _space_name_: space number or name to update a record;\n
            _key_: key that identifies a record;\n
            _op_list_: operation or list of operations. Each operation is tuple of three (or more) values;\n
            _key_type_: type of the key;\n
            _kwargs_: additional params;\n

        *Returns:*\n
            Tarantool server response.

        *Example:* (list of operations)\n
            | ${operation1}= | Create Operation | operation== | field=${1} | arg=NEW DATA |
            | ${operation2}= | Create Operation | operation== | field=${2} | arg=ANOTHER NEW DATA |
            | ${op_list}= | Create List | ${operation1} | ${operation2} |
            | Update | space_name=${SPACE_NAME} | key=${key} | op_list=${op_list} |

        *Example:* (one operation)\n
            | ${list_to_append}= | Create List | ${offset} | ${count} | ${value} |
            | ${operation}= | Create Operation | operation== | field=${1} | arg=NEW DATA |
            | Update | space_name=${SPACE_NAME} | key=${key} | op_list=${operation} |
        """
        logger.debug('Update data in space {space} with key {key} with operations {op_list}'.format(
            space=space_name,
            key=key,
            op_list=op_list
        ))
        if key_type:
            key = self._modify_key_type(key=key, key_type=key_type)
        if isinstance(op_list[0], (list, tuple)):
            return self._connection.update(space_name=space_name, key=key, op_list=op_list, **kwargs)
        else:
            return self._connection.update(space_name=space_name, key=key, op_list=[op_list], **kwargs)

    def delete(self, space_name, key, key_type=None, **kwargs):
        """
        Execute delete request.

        *Args:*\n
            _space_name_: space number or name to delete a record;\n
            _key_: key that identifies a record;\n
            _key_type_: type of the key;\n
            _kwargs_: additional params;\n

        *Returns:*\n
            Tarantool server response.

        *Example:*\n
            | Delete | space_name=${SPACE_NAME}| key=${key} |
        """
        logger.debug('Delete data in space {space} by key {key}'.format(space=space_name, key=key))
        if key_type:
            key = self._modify_key_type(key=key, key_type=key_type)
        return self._connection.delete(space_name=space_name, key=key, **kwargs)
