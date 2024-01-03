#!/usr/bin/env python
import re
import sys
import shutil
import traceback
from typing import Optional, List, Tuple, Dict, Any

import typer
from rich import print
from rich.columns import Columns
from rich.console import Console
from rich.traceback import install

# fmt: off
# Mapping from topics to colors
TOPICS = {
    "TIMR": "#9a9a99",
    "VOTE": "#67a0b2",
    "LEAD": "#d0b343",
    "TERM": "#70c43f",
    "LOG1": "#4878bc",
    "LOG2": "#398280",
    "CMIT": "#98719f",
    "PERS": "#d08341",
    "SNAP": "#FD971F",
    "DROP": "#ff615c",
    "CLNT": "#00813c",
    "TEST": "#fe2c79",
    "INFO": "#ffffff",
    "WARN": "#d08341",
    "ERRO": "#fe2626",
    "TRCE": "#fe2626",
}
# fmt: on


def list_topics(value: Optional[str]):
    if value is None:
        return value
    topics = value.split(",")
    for topic in topics:
        if topic not in TOPICS:
            raise typer.BadParameter(f"topic {topic} not recognized")
    return topics

class MetricsNode:
    def __init__(self):
        self.val:Any = None
        self.children:Dict[str, MetricsNode] = {}

class MetricsTimeEvent:
    def __init__(self, time: int, title: str):
        self.time = time
        self.title = title

class MetricsTimeline:
    def __init__(self):
        self.events: List[MetricsTimeEvent] = []

class TreeMetrics:
    def __init__(self):
        self.root = MetricsNode()

    def get_node(self, path: str) -> MetricsNode:
        if len(path) > 0 and path[0] == '/':
            path = path[1:]
        if len(path) > 0 and path[-1] == '/':
            path = path[:-1]
        dirs = path.split('/')
        node = self.root
        for d in dirs:
            if d not in node.children:
                node.children[d] = MetricsNode()
            node = node.children[d]
        return node

    def add_time_event(self, path: str, time: int, title: str):
        node = self.get_node(path)
        if node.val is None:
            node.val = MetricsTimeEvent(time, title)

    def add_timeline(self, path: str, time: int, title: str):
        node = self.get_node(path)
        if node.val is None:
            node.val = MetricsTimeline()
        for e in node.val.events:
            if e.title == title:
                return
        node.val.events.append(MetricsTimeEvent(time, title))

    def report_node(self, node: MetricsNode, depth: int):
        if type(node.val) is MetricsTimeEvent:
            print(f"{node.val.time:0=9d} {'|    ' * depth}|--- {node.val.title}")
        elif type(node.val) is MetricsTimeline:
            print(f"{node.val.events[0].time:0=9d} {'|    ' * depth}|--- {node.val.events[0].title}", end=' ')
            for i, e in enumerate(node.val.events[1:]):
                print(f"---- {e.time - node.val.events[i].time}μs ---- {e.title}", end=' ')
            print()
        elif node.val is None:
            depth -= 1
        for _, child in node.children.items():
            self.report_node(child, depth + 1)
    
    def report(self, path: str):
        node = self.get_node(path)
        self.report_node(node, 0)

LOG_EVENTS_DICT = {
    "SendApp": "Sending AppendEntries",
    "RecvApp": "Receiving AppendEntries",
    "FinishApp": "Finished AppendEntries",
    "AppReturn": "AppendEntries Return",
    "RecvVote": "Receiving RequestVote",
    "VoteReturn": "RequestVote Return",
    "RecvSnap": "Receiving InstallSnapshot",
    "SnapReturn": "InstallSnapshot Return",
    "StartCmd": "Starting Command",
    "StartSnap": "Starting Snapshot",
    "ElecTimeout": "Election Timeout",
    "HeartTimeout": "Heartbeat Timeout",
    "StorageComplete": "Storage Completed",
    "Shutdown": "Shutdown",
    "ApplySnap": "Applying Snapshot",
    "Apply": "Applying",
}
#LogPrint(dLeader, "S%d Sending AppendEntries To: S%d Term: %d PrevLogIndex: %d PrevLogTerm: %d Entries: %d-%d LeaderCommit: %d", rf.me, server, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.PrevLogIndex + 1, args.PrevLogIndex + len(args.Entries), args.LeaderCommit)
#LogPrint(dLog2, "S%d Receiving AppendEntries From: S%d Term: %d PrevLogIndex: %d PrevLogTerm: %d Entries: %d-%d LeaderCommit: %d RequestTerm: %d", rf.me, req.args.LeaderId, rf.store.CurrentTerm(), req.args.PrevLogIndex, req.args.PrevLogTerm, req.args.PrevLogIndex + 1, req.args.PrevLogIndex + len(req.args.Entries), req.args.LeaderCommit, req.args.Term)
#LogPrint(dLog2, "S%d Finished AppendEntries From S%d Term: %d Success: %t PrevLogIndex: %d PrevLogTerm: %d Entries: %d-%d LeaderCommit: %d RequestTerm: %d ResponseTerm: %d", rf.me, req.args.LeaderId, rf.store.CurrentTerm(), req.reply.Success, req.args.PrevLogIndex, req.args.PrevLogTerm, req.args.PrevLogIndex + 1, req.args.PrevLogIndex + len(req.args.Entries), req.args.LeaderCommit, req.args.Term, req.reply.Term)
#LogPrint(dLog, "S%d AppendEntries Return From S%d Term: %d Success: %t PrevLogIndex: %d PrevLogTerm: %d Entries: %d-%d LeaderCommit: %d RequestTerm: %d ResponseTerm: %d", rf.me, resp.server, rf.store.CurrentTerm(), resp.reply.Success, resp.args.PrevLogIndex, resp.args.PrevLogTerm, resp.args.PrevLogIndex + 1, resp.args.PrevLogIndex + len(resp.args.Entries), resp.args.LeaderCommit, resp.args.Term, resp.reply.Term)
#LogPrint(dVote, "S%d Receiving RequestVote From: S%d Term: %d RequestTerm: %d", rf.me, req.args.CandidateId, rf.store.CurrentTerm(), req.args.Term)
#LogPrint(dVote, "S%d RequestVote Return From S%d Term: %d VoteGranted: %t RequestTerm: %d ResponseTerm: %d", rf.me, resp.server, rf.store.CurrentTerm(), resp.reply.VoteGranted, resp.args.Term, resp.reply.Term)
#LogPrint(dSnap, "S%d Receiving InstallSnapshot From: S%d Term: %d LastIncludedIndex: %d LastIncludedTerm: %d RequestTerm: %d", rf.me, req.args.LeaderId, rf.store.CurrentTerm(), req.args.LastIncludedIndex, req.args.LastIncludedTerm, req.args.Term)
#LogPrint(dSnap, "S%d InstallSnapshot Return From S%d Term: %d LastIncludedIndex: %d LastIncludedTerm: %d RequestTerm: %d ResponseTerm: %d", rf.me, req.server, rf.store.CurrentTerm(), req.args.LastIncludedIndex, req.args.LastIncludedTerm, req.args.Term, req.reply.Term)
#LogPrint(dLeader, "S%d Starting Command Term: %d Index: %d-%d", rf.me, rf.store.CurrentTerm(), rf.store.LastLogIndex() + 1, rf.store.LastLogIndex() + len(batchReq) - 1)
#LogPrint(dSnap, "S%d Starting Snapshot Term: %d Index: %d", rf.me, rf.store.CurrentTerm(), req.index)
#LogPrint(dTimer, "S%d Election Timeout Term: %d", rf.me, rf.store.CurrentTerm())
#LogPrint(dTimer, "S%d Heartbeat Timeout Term: %d", rf.me, rf.store.CurrentTerm())
#LogPrint(dPersist, "S%d Storage Completed Term:%d Memory: %d-%d Storage: %d-%d", rf.me, rf.store.CurrentTerm(), rf.store.FirstLogIndex(), rf.store.LastLogIndex(), resp.firstLogIndex, resp.lastLogIndex)
#LogPrint(dInfo, "S%d Shutdown", rf.me)
#LogPrint(dCommit, "S%d Applying Snapshot LastIndex: %d LastTerm: %d", rf.me, req.index, req.term)
#LogPrint(dCommit, "S%d Applying Command: %d-%d", rf.me, req.index, req.index + len(req.entries) - 1)

class LogEvent:
    MessagePattern = "|".join(re.escape(event) for event in LOG_EVENTS_DICT.values())
    LogPattern = r'S(\d+) (' + MessagePattern + ')' + r'(.*)'
    AttrsSepPattern = r':|\s'

    @staticmethod
    def parse_attrs(attrs: str) -> Dict[str, str]:
        pairs = re.split(LogEvent.AttrsSepPattern, attrs)
        pairs = [p for p in pairs if p]
        return {pairs[i]: pairs[i + 1] for i in range(0, len(pairs), 2)}

    @staticmethod
    def parse_log(log: str) -> Tuple[int, str, Dict[str, str]]:
        match = re.match(LogEvent.LogPattern, log)
        if match is None:
            raise Exception("parse failed")
        return int(match.group(1)), match.group(2), LogEvent.parse_attrs(match.group(3))

class CommandProgressTracker:
    def __init__(self, metrics: TreeMetrics = TreeMetrics()):
        self.metrics = metrics
    def command_start(self, time: int, index: int, by: int):
        self.metrics.add_time_event(f"/cmd-{index}", time, f"Command {index}")
        self.metrics.add_time_event(f"/cmd-{index}/x", time, f"Start By S{by}")
        self.metrics.add_timeline(f"/cmd-{index}/x/{by}", time, f"Save On S{by}")
    
    def command_trace(self, time: int, index: int, by: int, title: str):
        self.metrics.add_timeline(f"/cmd-{index}/x/{by}", time, title)

    def command_report(self, index: int):
        self.metrics.report(f"/cmd-{index}")
    def reset(self):
        self.metrics = TreeMetrics()

def filter_line(line: str) -> bool:
    if len(line) == 0:
        return True
    if line[0].isdigit():
        return False
    return True

def main(
    file: typer.FileText = typer.Argument(None, help="File to read, stdin otherwise"),
    # colorize: bool = typer.Option(True, "--no-color"),
    # n_columns: Optional[int] = typer.Option(None, "--columns", "-c"),
    # ignore: Optional[str] = typer.Option(None, "--ignore", "-i", callback=list_topics),
    # just: Optional[str] = typer.Option(None, "--just", "-j", callback=list_topics),
):
    # topics = list(TOPICS)

    # We can take input from a stdin (pipes) or from a file
    input_ = file if file else sys.stdin
    # Print just some topics or exclude some topics (good for avoiding verbose ones)
    # if just:
    #     topics = just
    # if ignore:
    #     topics = [lvl for lvl in topics if lvl not in set(ignore)]

    # topics = set(topics)
    # console = Console()
    # width = console.size.width
    tracker = CommandProgressTracker()
    start_index = set()
    apply_index = set()
    # panic = False
    for line in input_:
        try:
            if filter_line(line):
                continue
            time, topic, *msg = line.strip().split(" ")
            # To ignore some topics
            # if topic not in topics:
            #     continue
            
            time = int(time)
            msg = " ".join(msg)
            server, msg, attrs = LogEvent.parse_log(msg)
            server = int(server)
            if msg == LOG_EVENTS_DICT["StartCmd"]:
                _, end  = map(int, attrs["Index"].split('-'))
                tracker.command_start(time, end, server)
                start_index.add(end)
            elif msg == LOG_EVENTS_DICT["SendApp"]:
                _, end  = map(int, attrs["Entries"].split('-'))
                to = int(attrs["To"][1])
                tracker.command_trace(time, end, to, f"Send To {attrs['To']}")
            elif msg == LOG_EVENTS_DICT["RecvApp"]:
                _, end  = map(int, attrs["Entries"].split('-'))
                tracker.command_trace(time, end, server, "Receive")
            elif msg == LOG_EVENTS_DICT["FinishApp"]:
                _, end  = map(int, attrs["Entries"].split('-'))
                tracker.command_trace(time, end, server, "Finish")
            elif msg == LOG_EVENTS_DICT["AppReturn"]:
                _, end  = map(int, attrs["Entries"].split('-'))
                from_ = int(attrs["From"][1])
                tracker.command_trace(time, end, from_, "Return")
            elif msg == LOG_EVENTS_DICT["StorageComplete"]:
                _, end  = map(int, attrs["Storage"].split('-'))
                for i in start_index:
                    if i <= end:
                        tracker.command_trace(time, i, server, "Finish")
            elif msg == LOG_EVENTS_DICT["Apply"]:
                _, end  = map(int, attrs["Command"].split('-'))
                for i in start_index:
                    if i <= end:
                        tracker.command_trace(time, i, server, "Apply")
                        apply_index.add(end)
            elif msg == LOG_EVENTS_DICT["ElecTimeout"]:
                if int(attrs["Term"]) == 0:
                    tracker.reset()
                    start_index = set()
                    apply_index = set()
            # # Debug calls from the test suite aren't associated with
            # # any particular peer. Otherwise we can treat second column
            # # as peer id
            # if topic != "TEST":
            #     i = int(msg[1])

            # # Colorize output by using rich syntax when needed
            # if colorize and topic in TOPICS:
            #     color = TOPICS[topic]
            #     msg = f"[{color}]{msg}[/{color}]"

            # # Single column printing. Always the case for debug stmts in tests
            # if n_columns is None or topic == "TEST":
            #     print(time, msg)
            # # Multi column printing, timing is dropped to maximize horizontal
            # # space. Heavylifting is done through rich.column.Columns object
            # else:
            #     cols = ["" for _ in range(n_columns)]
            #     msg = "" + msg
            #     cols[i] = msg
            #     col_width = int(width / n_columns)
            #     cols = Columns(cols, width=col_width - 1, equal=True, expand=True)
            #     print(cols)
        except KeyboardInterrupt:
            return
        except Exception as e:
            # # Code from tests or panics does not follow format
            # # so we print it as is
            # if line.startswith("panic"):
            #     panic = True
            # # Output from tests is usually important so add a
            # # horizontal line with hashes to make it more obvious
            # if not panic:
            #     print("#" * console.width)
            # print(line, end="")
            print(e, line)
            traceback.print_exc()
            pass
    
    if apply_index:
            for i in sorted(list(apply_index)):
                tracker.command_report(i)


if __name__ == "__main__":
    typer.run(main)
