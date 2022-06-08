import datetime
import random
import shutil
import os

from DatabaseSoftware.Enum_variables import EnumVariables


class Horadim_DB_engine:
    def __init__(self):
        self.loadObjectList()

    objects = []
    file_root = "C:/Users/user/Documents/DatabaseProject/"
    metadata_file = "DB_Metadata.txt"
    is_metadata_file_locked = False
    data_delimiter = "|"
    column_types = [EnumVariables.column_type_str.value, EnumVariables.column_type_int.value]
    restricted_characters = '[@!#$%^&*()<>?/\|}{~:]'

    def loadObjectList(self):
        file_obj = open(self.file_root + self.metadata_file, 'r')
        lines = file_obj.readlines()
        for line in lines:
            object_meta = line.strip().split(self.data_delimiter)
            if len(object_meta) > 1:
                self.objects.append({'Name': object_meta[0]
                                        , "File": object_meta[1]
                                        , "Attribute_count": object_meta[2]
                                        , "Primary_key_position": object_meta[3]
                                        , "Attributes_with_type": object_meta[4:]
                                     }
                                    )
                pass
            pass
        pass

    def list_objects(self, log_object):
        for obj in self.objects:
            log_object.print_output(obj['Name'])
            pass
        return True

    def create_object(self, object_name, rest_commands, log_object):
        return_val = True
        attribute_count = int(rest_commands[0])
        primary_key_position = int(rest_commands[1])
        file_name = object_name + ".dat"

        # self.objects.append(object_name)
        self.objects.append({'Name': object_name
                                , "File": file_name
                                , "Attribute_count": attribute_count
                                , "Primary_key_position": primary_key_position
                                , "Attributes_with_type": rest_commands[2:]
                             }
                            )
        try:
            file_obj = open(self.file_root + self.metadata_file, 'a')
            pass
        except Exception as e:
            log_object.log_error(f"create_object-Cant operate on {self.file_root}{self.metadata_file}")
            log_object.log_error(e.__str__())
            return_val = False
        else:
            file_obj.write(f"\n")
            out_text = ""
            for e in rest_commands[2:]:
                out_text += e + self.data_delimiter
            file_obj.write(
                f"{object_name}{self.data_delimiter}{file_name}{self.data_delimiter}{attribute_count}{self.data_delimiter}{primary_key_position}{self.data_delimiter}{out_text}")
        finally:
            file_obj.close()

        try:
            object_file = open(self.file_root + file_name, 'w')
        except Exception as e:
            log_object.log_error(f"create_object-Cant operate on {self.file_root}{file_name}")
            log_object.log_error(e.__str__())
            return_val = False
        else:
            object_file.write(f"\n")
            return_val = True
        finally:
            object_file.close()

        return return_val

    def drop_object(self, object_name, log_object):
        self.backup_metadata_file()
        is_obj_dropped = False
        for obj in self.objects:
            if obj["Name"] == object_name:
                os.remove(self.file_root + obj["File"])
                self.objects.remove(obj)
                file_obj = open(self.file_root + self.metadata_file, 'w')

                for element in self.objects:
                    out_text = ""
                    for i in element["Attributes_with_type"]:
                        out_text += self.data_delimiter + i

                    out_text = element["Name"] + self.data_delimiter + element["File"] + self.data_delimiter \
                               + str(element["Attribute_count"]) + self.data_delimiter \
                               + str(element["Primary_key_position"]) + out_text
                    file_obj.write(out_text + "\n")
                    out_text = ""

                file_obj.close()
                is_obj_dropped = True

        return is_obj_dropped

    def backup_metadata_file(self):
        unq=datetime.datetime.now().strftime("%y%m%d%H%M%S")+"_"+str(int(random.random()*1000000000))
        shutil.copyfile(src=self.file_root+self.metadata_file, dst=self.file_root+"bkp"+self.metadata_file+"_"+unq);

    def list_record(self, object_name, log_object):
        is_obj_data_read = False

        if not self.is_object_availabile(object_name, log_object):
            log_object.log_error(f"List record failed - Object doesnt exists {object_name}")
            is_obj_data_read = False
        else:
            for obj in self.objects:
                if obj["Name"] == object_name:
                    file_obj = open(self.file_root + obj["File"])
                    file_data = file_obj.readlines()
                    file_obj.close()

                    for line in file_data:
                        out_text = ""

                        for attr in line.strip().split(self.data_delimiter):
                            out_text += attr + " "
                            pass

                        log_object.print_output(out_text)

                    is_obj_data_read = True

        return is_obj_data_read

    def insert_record(self, object_name, attr_values, log_object):
        error_occurred = False
        if not self.is_object_availabile(object_name, log_object):
            log_object.log_error(f"Insert failed - Object doesnt exists {object_name}")
            error_occurred = True
        else:
            for obj in self.objects:
                if obj["Name"] == object_name:
                    attribute_count = obj["Attribute_count"]
                    primary_key_position = obj["Primary_key_position"]

                    # Validate input format - number of attributes
                    if len(attr_values) != int(attribute_count):
                        log_object.log_error(
                            f"Insert failed - attributes count is {attribute_count}, Supplied {len(attr_values)}")
                        error_occurred = True
                    else:
                        # Validate input format - input type
                        for i in range(1, int(attribute_count)):
                            attribute_type = obj["Attributes_with_type"][i * 2]
                            attr_value = attr_values[i]

                            if attribute_type == EnumVariables.column_type_int.value and not str(attr_value).isnumeric():
                                error_occurred = True
                                log_object.log_error(
                                    f"Insert failed - Invalid data type, Expected {attribute_type}, Supplied value is {attr_value}")
                            elif attribute_type == EnumVariables.column_type_str.value:
                                pass

                    if not error_occurred:
                        out_text = ""
                        for attribute in attr_values:
                            out_text += attribute + self.data_delimiter
                            pass

                        file_obj = open(self.file_root + obj["File"], 'a')
                        file_obj.write(out_text + "\n")
                        file_obj.close()
                        log_object.log_info("Insert succeeded - record inserted -" + out_text)

                        error_occurred = False
        return not error_occurred

    def update_record(self, object_name, pk_attr_value, attr_values, log_object):
        is_record_updated = True
        updated_record_count = 0
        #TODO: column type and size validation pending

        if not self.is_object_availabile(object_name, log_object):
            log_object.log_error(f"Update record failed - Object doesnt exists {object_name}")
            is_record_updated = False
        else:
            for obj in self.objects:
                if obj["Name"] == object_name:
                    file_obj = open(self.file_root + obj["File"])
                    file_data = file_obj.readlines()
                    file_obj.close()
                    object_data = []
                    pk_position = int(obj["Primary_key_position"])

                    for line in file_data:
                        record = list(filter(lambda x: x != "", line.strip().split(self.data_delimiter)))
                        if len(record) >= pk_position and record[pk_position - 1] == pk_attr_value:
                            object_data.append(attr_values)
                            updated_record_count += 1
                        else:
                            object_data.append(record)

                    file_obj = open(self.file_root + obj["File"], 'w')

                    for element in object_data:
                        out_text = ""
                        for attr in element:
                            out_text += attr + self.data_delimiter
                            pass
                        file_obj.writelines(out_text + "\n")

                    file_obj.close()
                    log_object.log_info(f"{updated_record_count} record(s) is/are updated in {object_name}")

        return is_record_updated

    def delete_record(self, object_name, pk_attr_value, log_object):
        is_record_deleted = True
        deleted_record_count = 0

        if not self.is_object_availabile(object_name, log_object):
            log_object.log_error(f"Delete record failed - Object doesnt exists {object_name}")
            is_record_deleted = False
        else:
            for obj in self.objects:
                if obj["Name"] == object_name:
                    file_obj = open(self.file_root + obj["File"])
                    file_data = file_obj.readlines()
                    file_obj.close()
                    object_data = []
                    pk_position = int(obj["Primary_key_position"])

                    for line in file_data:
                        record = list(filter(lambda x: x != "", line.strip().split(self.data_delimiter)))
                        if len(record) >= pk_position and record[pk_position - 1] == pk_attr_value:
                            deleted_record_count += 1
                        else:
                            object_data.append(record)

                    file_obj = open(self.file_root + obj["File"], 'w')

                    for element in object_data:
                        out_text = ""
                        for attr in element:
                            out_text += attr + self.data_delimiter
                            pass
                        file_obj.writelines(out_text + "\n")

                    file_obj.close()
                    log_object.log_info(f"{deleted_record_count} record(s) is/are deleted in {object_name}")

        return is_record_deleted

    def is_object_availabile(self, object_name, log_object):
        is_obj_available = False
        for obj in self.objects:
            if obj["Name"] == object_name:
                is_obj_available = True

        return is_obj_available

    def is_valid_attribute_type(self, attributes_list_with_type, log_object):
        invalid_attribute_type = False
        column_types = [attributes_list_with_type[i] for i in range(1, len(attributes_list_with_type), 2)]
        for element in column_types:
            if not element in self.column_types:
                invalid_attribute_type = True
                log_object.log_error(f"Syntax error - Invalid column type -{element}", "error")

        return not invalid_attribute_type

    def is_attribute_names_unique(self, attributes_list_with_type, log_object):
        is_attribute_name_repeats = False
        attribute_list = [attributes_list_with_type[i] for i in range(0, len(attributes_list_with_type), 2)]

        for i in range(len(attribute_list)):
            if attribute_list[i] in attribute_list[i + 1:len(attribute_list)]:
                is_attribute_name_repeats = True
                log_object.log_error(f"Syntax error - attribute name repeats -{attribute_list[i]}", "error")

        return not is_attribute_name_repeats

    def is_attributes_name_valid(self, attributes_list_with_type, log_object):
        is_attribute_name_valid = True
        attribute_list = [attributes_list_with_type[i] for i in range(0, len(attributes_list_with_type), 2)]

        for attribute in attribute_list:
            if any(characters in self.restricted_characters for characters in attribute):
                is_attribute_name_valid = False
                log_object.log_error(f"Syntax error - Special characters is/are found in attribute name  -{attribute}",
                                     "error")
                pass
            pass

        return is_attribute_name_valid

    def is_create_object_command_valid(self, object_name, rest_command, log_object):
        error_occurred = False
        attribute_count = rest_command[0] if len(rest_command) >= 1 else None
        primary_key_position = rest_command[1] if len(rest_command) >= 2 else None
        if object_name is None:
            error_occurred = True
        elif any(char in self.restricted_characters for char in object_name):
            log_object.log_error(f"Can not create type {object_name}- Special characters are not allowed other than '_'", status="error")
            error_occurred = True
        elif self.is_object_availabile(object_name, log_object):
            log_object.log_error(f"Can not create type {object_name}- Object already exist", status="error")
            error_occurred = True
        elif rest_command is None:
            log_object.log_error(f"Can not create type {object_name}- Syntax error", status="error")
            error_occurred = True
        elif attribute_count is None or not str(attribute_count).isnumeric() or int(attribute_count) < 1:
            log_object.log_error(f"Can not create type {object_name}-Column Count is either not supplied or non "
                                 f"numeric or 0", status="ERROR")
            error_occurred = True
        elif primary_key_position is None or not (str(primary_key_position).isnumeric()):
            log_object.log_error(f"Can not create type {object_name}-primary key column position is either not "
                                 f"supplied or non numeric",
                                 status="error")
            error_occurred = True
        elif primary_key_position > attribute_count:
            log_object.log_error(f"Can not create type {object_name}- Primary Key attribute position {primary_key_position} is beyond "
                                 f"attribute count {attribute_count}", status="error")
            error_occurred = True
        elif len(rest_command) != 2 * int(attribute_count) + 2:
            log_object.log_error(f"Can not create type {object_name}- Attribute count is not matching with number of attributes",
                                 status="ERROR")
            error_occurred = True
        elif len(rest_command) <= 2 or len(rest_command) % 2 != 0:
            log_object.log_error(f"Can not create type {object_name}-Syntax error", status="error")
            error_occurred = True
        elif not self.is_valid_attribute_type(rest_command[2:len(rest_command)], log_object):
            error_occurred = True
        elif not self.is_attribute_names_unique(rest_command[2:len(rest_command)], log_object):
            error_occurred = True
        elif not self.is_attributes_name_valid(rest_command[2:len(rest_command)], log_object):
            error_occurred = True
        else:
            pass

        return not error_occurred
