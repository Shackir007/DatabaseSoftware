import datetime as dt

class HoradimLogs:
    def __init__(self):
        super(HoradimLogs, self).__init__()

    log_file_name_with_path = "C:\\Users\\user\\Documents\\DatabaseProject\\DBLog.txt"
    debug_level_error = True
    debug_level_info = False
    debug_level_debug = False

    def set_debug_level(self, debug_level):
        if (debug_level == "error"):
            self.debug_level_error = True
            self.debug_level_info = False
            self.debug_level_debug = False
            pass
        elif (debug_level == "info"):
            self.debug_level_error = True
            self.debug_level_info = True
            self.debug_level_debug = False
            pass
        elif (debug_level == "debug"):
            self.debug_level_error = True
            self.debug_level_info = True
            self.debug_level_debug = True
            pass

        self.log_info(status=None, message="Current Debug Level is " + debug_level + " and values are error:" + str(
            self.debug_level_error) + " ,debug:" + str(self.debug_level_debug) + ", info:" + str(self.debug_level_info))

    def log_error(self, message, status="ERROR"):
        if self.debug_level_error:
            print(message,"ERROR",status)
            self.append_log(message=message, log_type="ERROR", status=status)
            pass
        pass

    def log_debug(self, message, status="DEBUG"):
        if self.debug_level_debug:
            self.append_log(message=message, log_type="DEBUG", status=status)
            pass
        pass

    def log_info(self, message, status="INFO"):
        if self.debug_level_info:
            self.append_log(message=message, log_type="INFO", status=status)
            pass
        pass

    def append_log(self, message, log_type, status):
        current_time = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        file_object = open(self.log_file_name_with_path, 'a')
        #print(current_time, message, status, type)
        file_object.write(f"{current_time}-{log_type}-{message} {status or ''} \n")
        pass

    def print_output(self, message):
        #print("------Output-------")
        print(message)

    def test_this_class(self):
        self.set_debug_level("error")
        self.log_debug(status=None, message="Its a debug")
        self.log_error(status=None, message="Its an error")
        self.log_info(status=None, message="Its an info")

        self.set_debug_level("debug")
        self.log_debug(status=None, message="Its a debug")
        self.log_error(status=None, message="Its an error")
        self.log_info(status=None, message="Its an info")

        self.set_debug_level("info")
        self.log_debug(status=None, message="Its a debug")
        self.log_error(status=None, message="Its an error")
        self.log_info(status=None, message="Its an info")
        pass

#obj = HoradimLogs()
#obj.test_this_class()
