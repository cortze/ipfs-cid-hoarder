from datetime import datetime


# List of possible results:
# 0 -> No Result
# 1 -> Provider Address Info Without Multiaddress
# 2 -> Provider Address Info With Multiaddress
no_provider         = 0
provider_no_maddr   = 1
provider_with_maddr = 2 

# log types
cid_generation  = 0
cid_pinging     = 1
indv_ping       = 2
indv_lookup     = 3
lookup          = 4
finish          = 5

def convert_logrustime_to_unix(ltime:str):
    date = ltime.split("+")[0]
    t = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")
    return t.timestamp()

def convert_fmttime_to_unix(ftime):
    t = datetime.strptime(ftime, "%Y-%m-%d %H:%M:%S")
    return t.timestamp()



class LogLine():
    # list of log types
    cid_generation_log  = "generated new CID"
    cid_pinging_log     = "pinging CID"
    indv_ping_log       = "reporting on"
    indv_lookup_log     = "had 1 providers for"
    lookup_log          = "Providers for"
    finish              = "finished pinging CID"

    def __init__(self, line_text):
        self.raw_line = line_text

    def classify_log(self):
        type = -1
        if self.cid_generation_log in self.raw_line:
            type = 0
        # priority from pining
        elif self.finish in self.raw_line:
            type = 5
        elif self.cid_pinging_log in self.raw_line:
            type = 1
        elif self.indv_ping_log in self.raw_line:
            type = 2
        elif self.indv_lookup_log in self.raw_line:
            type = 3
        elif self.lookup_log in self.raw_line:
            type = 4
        return type 

    # time="2022-09-21T18:12:36+02:00" level=info msg="generated new CID QmTJNoEqpg7A7hz4g2bWqTB16iLaeLfW7b76bov73x6DqC"
    def parse_gen_log(self):
        # split line
        spline = self.raw_line.split('"')
        # get gen-time
        gen_time = convert_logrustime_to_unix(spline[1])
        # get CID
        cid = spline[3].replace("generated new CID ", "")
        return cid, gen_time

    # time="2022-09-21T18:16:47+02:00" level=info msg="pinging CID QmTJNoEqpg7A7hz4g2bWqTB16iLaeLfW7b76bov73x6DqC created by %!d(string=16Uiu2HAm48ESyRKK7G9Z4wecwaPttQNqJXhAFd42CZC8xXFUwkb1) | round 1 from host %!d(string=16Uiu2HAm58zucx23sbESvBVxVXyEekZtDhbuGuLx5dwZtpwsLfFS)" pinger=0
    def parse_ping_not(self):
        # split line
        spline = self.raw_line.split('"')
        # get gen-time
        ping_time = convert_logrustime_to_unix(spline[1])
        # get CID
        cid = spline[3].split(" ")[2]
        return cid, ping_time
    
    # 2022-09-21 18:16:47.469701563 +0200 CEST m=+253.393126266 Peer 12D3KooWNVBT7H1zEigXG7pT2WwjiFx7AD32DAHx9HNiNVsYzYpJ reporting on QmTJNoEqpg7A7hz4g2bWqTB16iLaeLfW7b76bov73x6DqC  ->  {16Uiu2HAm48ESyRKK7G9Z4wecwaPttQNqJXhAFd42CZC8xXFUwkb1: [/ip4/192.168.20.58/tcp/9010 /ip4/127.0.0.1/tcp/9010 /ip4/84.88.187.121/tcp/9010]}
    def parse_indv_ping(self):
        # split line
        spline = self.raw_line.split(' ')
        # get gen-time
        cnt_str = f"{spline[0]} {spline[1]}"  # 2022-09-21 18:16:47.469701563
        ping_time = convert_fmttime_to_unix(cnt_str.split(".")[0]) # 2022-09-21 18:16:47
        # get CID
        cid = spline[9]
        # get status
        status = 0
        # l = self.raw_line.replace(']}', '') # Remove last characteres
        spline = self.raw_line.split('[')
        maddress = spline[1].split(' ') # split maddr field by spaces ''
        status = 0
        if len(maddress) == 1:
            if maddress[0] == ']}':
                status = 1

        elif len(maddress) > 1:
            status = 2

        return cid, ping_time, status

    # peer 12D3KooWGxNtynvCJgDgdYWgBBWsr1SWev7At8E2wF8mwepdBiA9 had 1 providers for  QmTJNoEqpg7A7hz4g2bWqTB16iLaeLfW7b76bov73x6DqC | 2022-09-21 18:24:48.577773442 +0200 CEST m=+734.501198139 -> [{16Uiu2HAm48ESyRKK7G9Z4wecwaPttQNqJXhAFd42CZC8xXFUwkb1: []}]
    def parse_lookup_ping(self):
        # split line
        spline = self.raw_line.split(' ')
        # get gen-time
        cnt_str = f"{spline[9]} {spline[10]}"  # 2022-09-21 18:16:47.469701563
        ping_time = convert_fmttime_to_unix(cnt_str.split(".")[0]) # 2022-09-21 18:16:47
        # get CID
        cid = spline[7]
        # get status
        status = 0
        # l = self.raw_line.replace(']}', '') # Remove last characteres
        spline = self.raw_line.split('[')
        maddress = spline[2].split(' ') # split maddr field by spaces ''
        status = 0
        if len(maddress) == 1:
            if maddress[0] == ']}]':
                status = 1

        elif len(maddress) > 1:
            status = 2

        return cid, ping_time, status

    def parse_lookup(self):
        # split line
        spline = self.raw_line.split(' ')
        # get gen-time
        cnt_str = f"{spline[0]} {spline[1]}"  # 2022-09-21 18:16:47.469701563
        ping_time = convert_fmttime_to_unix(cnt_str.split(".")[0]) # 2022-09-21 18:16:47
        # get CID
        cid = spline[7]
        # get status
        status = 0
        # l = self.raw_line.replace(']}', '') # Remove last characteres
        spline = self.raw_line.split('[')
        maddress = spline[2].split(' ') # split maddr field by spaces ''
        status = 0
        if len(maddress) == 1:
            if maddress[0] == ']}]':
                status = 1

        elif len(maddress) > 1:
            status = 2

        return cid, ping_time, status


class LogFile():
    def __init__(self, log_file):
        self.file_name = log_file
        self.cid_map = {}
        self.publisher_id = ""
        self.pinger_id = ""
        # Log-Related times
        self.start_time = 0     # UnixTimestamp
        self.finish_time = 0    # UnixTimestamp
        self.total_duration = 0 # Seconds

        # Ping-Rounds related data
        self.ping_rounds = []  

    def analyze_logs(self):

        with open(self.file_name) as f:
            lines = f.readlines()

        for i, line in enumerate(lines):
            try:
                line = line.replace("\n", "")

                log_line = LogLine(line)
                log_type = log_line.classify_log()

                if log_type == cid_generation :
                    cid, gen_time = log_line.parse_gen_log()

                    # Generate new CID obj
                    cid_obj = CID(cid, gen_time)
                    self.cid_map[cid] = cid_obj
                    continue

                elif log_type == finish:
                    continue 

                elif log_type == cid_pinging :
                    cid, t = log_line.parse_ping_not()
                    # check if the cid is already in the map (if we are using the discoverer won't be there)
                    if cid not in self.cid_map: 
                        # generate a new entry in the map for this discovered cid
                        cid_obj = CID(cid, 0) 
                        # add the cid to the 
                        self.cid_map[cid] = cid_obj

                    self.cid_map[cid].add_new_ping_round(t)
                    continue       
                    
                elif log_type == indv_ping :
                    cid, t, status = log_line.parse_indv_ping()
                    self.cid_map[cid].add_indv_ping_res(status, t)
                    continue 

                elif log_type == indv_lookup :
                    cid, t, status = log_line.parse_lookup_ping()
                    self.cid_map[cid].add_indv_lookup_res(status, t)
                    continue 

                elif log_type == lookup :
                    cid, t, status = log_line.parse_lookup()
                    self.cid_map[cid].add_lookup_res(status, t)
                    continue 
        
            except Exception as e:
                print(f"error occurred at line {i}")
                print(e)

        f.close() 
        print("done - no errors")


class CID ():

    def __init__(self, cid, gen_time):
        self.cid = cid
        self.gen_time = gen_time
        self.ping_rounds = []


    def add_new_ping_round(self, ping_start_time):
        pr = PingRound(ping_start_time)
        self.ping_rounds.append(pr)

    def last_ping_round(self):
        return len(self.ping_rounds) - 1

    def add_indv_ping_res(self, status, t):
        pr_idx = self.last_ping_round()
        if pr_idx < 0:
            raise "no ping round"
        # Add new result to pr 
        self.ping_rounds[pr_idx].new_indv_ping(status, t)

    def add_indv_lookup_res(self, status, t):
        pr_idx = self.last_ping_round()
        if pr_idx < 0:
            raise "no ping round"
        # Add new result to pr 
        self.ping_rounds[pr_idx].new_lookup_indv_res(status, t)

    def add_lookup_res(self, status, t):
        pr_idx = self.last_ping_round()
        if pr_idx < 0:
            raise "no ping round"
        # Add new result to pr 
        self.ping_rounds[pr_idx].new_lookup_res(status, t)

class PingRound():
    def __init__(self, start_time):
        self.start_time = start_time    # Unix Timestamp
        self.stop_time = 0              # Unix Timestamp
        self.total_duration = 0         # Seconds
        # Results
        self.pr_holder_succ_pings = 0
        self.pr_holder_ping_with_multiaddr = 0
        self.lookup_succ_pings =  0
        self.lookup_succ_pings_with_multiaddr =  0
        self.lookup_final_result = 0

    def new_indv_ping(self, status, ping_time):
        if status == no_provider:
            # so far, not do anything
            return 

        self.pr_holder_succ_pings += 1

        if status == provider_with_maddr:
            self.pr_holder_ping_with_multiaddr += 1

        # independently of the status - update finish time
        self.stop_time = ping_time
        self.duration = self.stop_time - self.start_time


    def new_lookup_indv_res(self, status, ping_time):
        if status == no_provider:
            # so far, not do anything
            return 

        self.lookup_succ_pings += 1
        
        if status == provider_with_maddr:
            self.lookup_succ_pings_with_multiaddr += 1

        # independently of the status - update finish time
        self.stop_time = ping_time
        self.duration = self.stop_time - self.start_time 

    def new_lookup_res(self, status, res_time):
        # copy the status 
        self.lookup_final_result=status

        # independently of the status - update finish time
        self.stop_time = res_time
        self.duration = self.stop_time - self.start_time  

    def summary(self):
        return {
            "start_time": self.start_time,
            "stop_time": self.stop_time,
            "total_duration": self.total_duration,
            "pr_holder_succ_pings": self.pr_holder_succ_pings,
            "pr_holder_ping_with_multiaddr": self.pr_holder_ping_with_multiaddr,
            "lookup_succ_pings": self.lookup_succ_pings,
            "lookup_succ_pings_with_multiaddr": self.lookup_succ_pings_with_multiaddr,
            "lookup_final_result": self.lookup_final_result,
            }
       
def test_time_parse():
    print(convert_logrustime_to_unix("2022-09-21T18:12:36+02:00"))
    print(convert_fmttime_to_unix("2022-09-21 18:16:47"))

def test_log_parser():
    
    # cid gen
    log = LogLine('time="2022-09-21T18:12:36+02:00" level=info msg="generated new CID QmTJNoEqpg7A7hz4g2bWqTB16iLaeLfW7b76bov73x6DqC"')
    cid, time = log.parse_gen_log()
    print("\n gen")
    print(log.raw_line)
    print(cid, time)

    cid = CID(cid, time)

    # ping start
    log2 = LogLine('time="2022-09-21T18:16:43+02:00" level=info msg="pinging CID QmTJNoEqpg7A7hz4g2bWqTB16iLaeLfW7b76bov73x6DqC created by %!d(string=16Uiu2HAm48ESyRKK7G9Z4wecwaPttQNqJXhAFd42CZC8xXFUwkb1) | round 1 from host %!d(string=16Uiu2HAm58zucx23sbESvBVxVXyEekZtDhbuGuLx5dwZtpwsLfFS)" pinger=9')
    cid, time = log2.parse_ping_not()
    print("\n ping")
    print(log2.raw_line)
    print(cid, time)


    # individual pings
    log3 = LogLine('2022-09-21 18:16:47.469701563 +0200 CEST m=+253.393126266 Peer 12D3KooWNVBT7H1zEigXG7pT2WwjiFx7AD32DAHx9HNiNVsYzYpJ reporting on QmTJNoEqpg7A7hz4g2bWqTB16iLaeLfW7b76bov73x6DqC  ->  {16Uiu2HAm48ESyRKK7G9Z4wecwaPttQNqJXhAFd42CZC8xXFUwkb1: [/ip4/192.168.20.58/tcp/9010 /ip4/127.0.0.1/tcp/9010 /ip4/84.88.187.121/tcp/9010]}')
    cid, time, status = log3.parse_indv_ping()
    print("\n indv ping")
    print(log3.raw_line)
    print(cid, time, status)

    log4 = LogLine('2022-09-21 18:24:48.444163643 +0200 CEST m=+734.367588322 Peer 12D3KooWQs38YiFmSvu4ute7uktuwCB3eHuH271iTRhEKF3ao4Kp reporting on QmTJNoEqpg7A7hz4g2bWqTB16iLaeLfW7b76bov73x6DqC  ->  {16Uiu2HAm48ESyRKK7G9Z4wecwaPttQNqJXhAFd42CZC8xXFUwkb1: []}')
    cid, time, status = log4.parse_indv_ping()
    print("\n indv ping")
    print(log4.raw_line)
    print(cid, time, status)

    # Lookup pings
    log5 = LogLine('peer 12D3KooWGxNtynvCJgDgdYWgBBWsr1SWev7At8E2wF8mwepdBiA9 had 1 providers for  QmTJNoEqpg7A7hz4g2bWqTB16iLaeLfW7b76bov73x6DqC | 2022-09-21 18:24:48.577773442 +0200 CEST m=+734.501198139 -> [{16Uiu2HAm48ESyRKK7G9Z4wecwaPttQNqJXhAFd42CZC8xXFUwkb1: []}]')
    cid, time, status = log5.parse_lookup_ping()
    print("\n lookup ping")
    print(log5.raw_line)
    print(cid, time, status)

    log6 = LogLine('peer 12D3KooWH2Sf94FG3sMBnLfgFSxNBmjFq9EeyzwKPZ7V71Rq9uYY had 1 providers for  QmTJNoEqpg7A7hz4g2bWqTB16iLaeLfW7b76bov73x6DqC | 2022-09-21 18:24:49.631525575 +0200 CEST m=+735.554950261 -> [{16Uiu2HAm48ESyRKK7G9Z4wecwaPttQNqJXhAFd42CZC8xXFUwkb1: [/ip4/192.168.20.58/tcp/9010 /ip4/127.0.0.1/tcp/9010 /ip4/84.88.187.121/tcp/9010]}]')
    cid, time, status = log6.parse_lookup_ping()
    print("\n lookup ping")
    print(log6.raw_line)
    print(cid, time, status)

    # Lookup
    log7 = LogLine('2022-09-21 18:56:52.436937642 +0200 CEST m=+2658.360362339 Providers for QmTJNoEqpg7A7hz4g2bWqTB16iLaeLfW7b76bov73x6DqC -> [{16Uiu2HAm48ESyRKK7G9Z4wecwaPttQNqJXhAFd42CZC8xXFUwkb1: []}]')
    cid, time, status = log7.parse_lookup()
    print("\n lookup")
    print(log7.raw_line)
    print(cid, time, status)

    log8 = LogLine('2022-09-21 18:32:56.08032241 +0200 CEST m=+1222.003747024 Providers for QmTJNoEqpg7A7hz4g2bWqTB16iLaeLfW7b76bov73x6DqC -> [{16Uiu2HAm48ESyRKK7G9Z4wecwaPttQNqJXhAFd42CZC8xXFUwkb1: [/ip4/192.168.20.58/tcp/9010 /ip4/127.0.0.1/tcp/9010 /ip4/84.88.187.121/tcp/9010]}]')
    cid, time, status = log8.parse_lookup()
    print("\n lookup ping")
    print(log8.raw_line)
    print(cid, time, status)


def main():
    log_file = "/home/cortze/devel/cortze/ipfs-cid-hoarder/parsed_logs.txt"

    lfile = LogFile(log_file)
    lfile.analyze_logs()

    print(lfile.cid_map["QmTJNoEqpg7A7hz4g2bWqTB16iLaeLfW7b76bov73x6DqC"].ping_rounds[9].summary())

if __name__ == "__main__":
    #time_test()
    #test_log_parser()

    main()