from DBLogs import *
from DB_engine import *


class CommandsExecution:
    def __init__(self):
        super(CommandsExecution, self).__init__()

    log_object = None
    horadimengineObj = None
    command = None
    action_list = ("create type", "list type", "drop type", "insert record", "update record", "delete record",
                   "search record", "list record", "filter record")

    def parseCommands(self, command):
        if not (command.strip().startswith("#")):
            self.command = str(command).split("\n")[0]
            self.log_object.log_debug(self.command)
            command_words = list(filter(lambda x: x != '', self.command.split(" ")))
            action = command_words[0]
            object_type = command_words[1] if len(command_words) > 1 else None
            object_name = command_words[2] if len(command_words) > 2 else None
            # log_object.print_output(command_words)

            if len(command_words) <2:
                return_value = False
                self.log_object.log_error("Syntax error - "+self.command)
            else:
                return_value = self.validate_action(action.lower() + " " + object_type.lower(), self.command)

            if return_value and object_name is not None:
                pass

            if return_value:
                return_value = self.execute_command(action.lower() + " " + object_type.lower(),
                                                    object_name.lower() if object_name is not None else None,
                                                    command_words[3:] if len(command_words) > 3 else None)

            if return_value:
                self.log_object.log_info(self.command, status="SUCCESS")
                self.log_object.print_output(f"SUCCESS - {self.command}")
            else:
                self.log_object.print_output(f"ERROR - {self.command}")
                self.log_object.log_error(self.command, status="ERROR")
            pass
        pass

    def executeCoammandsFromFile(self, input_file, output_file):
        try:
            input_file_obj = open(input_file, "r")
            output_file_obj = open(output_file, "w")
        except FileNotFoundError as f404:
            self.log_object.log_error("Could not able to find input file, File:" + str(input_file))
            self.log_object.log_debug(f404.__str__())
        else:
            command_list = input_file_obj.readlines()
            input_file_obj.close()
            for command in command_list:
                self.parseCommands(command)
        try:
            output_file_obj = open(output_file, "w")
        except FileNotFoundError as f404:
            self.log_object.log_error("Could not create output file, File:" + str(output_file))
            self.log_object.log_debug(f404.__str__())
        else:
            pass
        finally:
            output_file_obj.close()

    def help(self, command=None):
        message = """" Commands
---------------------------------------------------------------
help        - print command help
exit    - exit from application
#       - commented command line - No action
@       - source command from script file
          Eg: @C:\\Downloads\\script.txt -> C:\\Downloads\\output.txt

create type - create type/object in DB
              Eg: create type <type_name> <total attribute count> <Primary key attribute column position [>=0]> attribute_name attribute_type[str/int]
drop type   - drop type/object in DB
              Eg: drop type <type_name>
list type   - list all available types/objects in DB
              Eg: list type

insert record   - add a record in a type/object
update record   - update record(s) in a type/object
delete record   - delete record(s) in a type/object
list record     - list all available records from a type/object
---------------------------------------------------------------
                """

        self.log_object.print_output(message)
        pass

    def getUserInput(self):
        self.log_object.log_debug("Started command mode")

        while 1 == 1:
            ip = input("Enter Command here >>")
            self.log_object.log_debug(f"User input is {ip}")
            self.log_object.print_output(ip)

            if ip.strip().strip(" ") == "":
                pass
            elif ip.strip().strip(" ").startswith("#"):
                pass
            elif ip.strip().strip(" ") == "help":
                self.help()
            elif ip.strip().strip(" ") == "exit":
                self.log_object.log_debug("Exiting from command mode")
                break
            elif ip.strip().strip(" ").startswith("@"):
                if len(ip.strip().strip(" ").replace("@", " ").split("->")) >= 2:
                    input_file_nm = ip.strip().strip(" ").replace("@", " ").split("->")[0]
                    output_file_nm = ip.strip().strip(" ").replace("@", " ").split("->")[1]
                    self.log_object.log_info(
                        f"File command execution, command file:{input_file_nm}, output:{output_file_nm}")
                    self.executeCoammandsFromFile(input_file_nm.strip().strip(" "), output_file_nm.strip().strip(" "))
                else:
                    self.log_object.log_error("Syntax error: Output file name is required")
                    self.log_object.log_info("Command format: @commandfilewithpath -> outputfilewithpath")
            else:
                self.parseCommands(ip)
                pass

    def execute_command(self, action, object_name, rest_commands):
        return_value = False
        if action == "list type":
            if object_name is not None:
                self.log_object.log_error("list type - syntax error")
            else:
                return_value = self.horadimengineObj.list_objects(self.log_object)
        elif action == "create type":
            if not self.horadimengineObj.is_create_object_command_valid(object_name, rest_commands, self.log_object):
                return_value = False
            else:
                return_value = self.horadimengineObj.create_object(object_name, rest_commands, self.log_object)
        elif action == "drop type":
            if self.horadimengineObj.is_object_availabile(object_name, self.log_object):
                return_value = self.horadimengineObj.drop_object(object_name, self.log_object)
            else:
                return_value = False
                self.log_object.log_error("Error - Object doesnt exist", "ERRor")
        elif action == "list record":
            return_value = self.horadimengineObj.list_record(object_name, self.log_object)
        elif action == "insert record":
            return_value = self.horadimengineObj.insert_record(object_name, rest_commands, self.log_object)
        elif action == "update record":
            primary_key_val = rest_commands[0]
            update_records = rest_commands[1:]
            return_value = self.horadimengineObj.update_record(object_name, primary_key_val, update_records,
                                                               self.log_object)
        elif action == "delete record":
            primary_key_val = rest_commands[0]
            return_value = self.horadimengineObj.delete_record(object_name, primary_key_val, self.log_object)

        return return_value

    def validate_action(self, action, command):
        if len(list(filter(lambda x: x == action, self.action_list))) == 0:
            error_message = f"Invalid syntax: {command}"
            self.log_object.log_error(error_message, None)
            return False
        return True

    def write_output(self, message):
        output_file_obj = open(self.output_file_name, 'w')

    def start_db_engine(self):
        self.output_file_name = ""
        self.log_object = HoradimLogs()
        self.log_object.set_debug_level("debug")
        self.horadimengineObj = Horadim_DB_engine()
        self.horadimengineObj.backup_metadata_file()
        self.getUserInput()
        pass

    def test_this_class(self):
        """
        self.begin("C:/Users/user/Documents/Horadim/HoradrimCommands.txt",
                   "C:/Users/user/Documents/Horadim/HoradrimCommands_result.txt")
        """
        self.start_db_engine()


obj = CommandsExecution()
obj.test_this_class()
