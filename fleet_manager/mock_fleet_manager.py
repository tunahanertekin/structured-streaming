from time import time
from typing import List
import names, random, hashlib

class Robot:
    def __init__(self, id: int):
        self.id = id
        types = ["Cleaner", "Transporter", "Waiter"]
        self.type = types[random.randint(0,2)]
        if self.type == "Cleaner":
            self.task_types = ["Clean", "Wipe", "Wash", "Go"]
        elif self.type == "Transporter":
            self.task_types = ["Carry Package", "Go"]
        elif self.type == "Waiter":
            self.task_types = ["Serve Drink", "Serve Meal", "Go"]
        self.name = names.get_last_name()

    def sum(self):
        return str(self.id) + "/" + self.name + "/" + self.type

    def present(self):
        return "Robot: " + str(self.id) + "/" + self.name + "/" + self.type


def get_robots(num: int) -> List[Robot]:
    robots = []
    for i in range(num):
        robots.append(Robot(i))
    return robots

class Fleet:
    def __init__(self, name: str):
        self.name = name
        self.robots = get_robots(random.randint(12,20))

    def sum(self):
        return self.name

    def present(self):
        fleet_str = self.name
        for r in self.robots:
            fleet_str += "\n\t" + r.present()
        return fleet_str

def get_fleets(num: int, prefix: str) -> List[Fleet]:
    fleets = []
    for i in range(num):
        fleets.append(Fleet(prefix + ": " + "Fleet " + chr(65 + i)))
    return fleets


class Area:
    def __init__(self, name: str):
        self.name = name

    def sum(self):
        return self.name

    def present(self):
        return "Area: " + self.name

    
def get_areas(num: int) -> List[Area]:
    areas = []
    for i in range(num):
        areas.append(Area(chr(65+i))) # capital letters
    return areas

class Airport:
    def __init__(self, prefix: str, name: str):
        self.prefix = prefix
        self.name = name
        self.fleets = get_fleets(random.randint(2,4), prefix)
        self.areas = get_areas(random.randint(7,12))

    def sum(self):
        return self.prefix + " - " + self.name

    def present(self):
        ap_str = "Airport: " + self.prefix + " - " + self.name + "\n"
        for a in self.areas:
            ap_str += "\n" + a.present()
        for f in self.fleets:
            ap_str += "\n\n" + f.present()
        return ap_str

def get_airports() -> List[Airport]:
    airports = []
    airports.append(Airport("ESB", "Esenboga"))
    airports.append(Airport("JFK", "John F. Kennedy International"))
    airports.append(Airport("BER", "Berlin Brandenburg"))
    return airports

class Task:
    def __init__(self, id: str = None, airport: str = None, fleet: str = None, area: str = None, robot: str = None, task: str = None):
        self.id = id
        self.airport = airport
        self.fleet = fleet
        self.area = area
        self.robot = robot
        self.task = task
        self.priority = random.randint(1,10)
        self.status = "assigned"
        self.starting_time = time()
        self.finishing_time = 0

    def get_task_obj(task_str: str):
        attr = task_str.split(",")
        t = Task()
        t.id = attr[0]
        t.airport = attr[1]
        t.fleet = attr[2]
        t.area = attr[3]
        t.robot = attr[4]
        t.task = attr[5]
        t.priority = attr[6]
        t.status = attr[7]
        t.starting_time = attr[8]
        t.finishing_time = attr[9]
        return t        

    def present(self):
        task_str = "-------------\n"
        task_str += "Assigned Task"
        task_str += "\n" + "ID:\t" + str(self.id)
        task_str += "\n" + "Airport:\t" + self.airport
        task_str += "\n" + "Fleet:\t" + self.fleet
        task_str += "\n" + "Area:\t" + self.area
        task_str += "\n" + "Robot:\t" + self.robot
        task_str += "\n" + "Task:\t" + self.task
        task_str += "\n" + "Priority:\t" + str(self.priority)
        task_str += "\n" + "Status:\t" + self.status
        task_str += "\n" + "Starting Time:\t" + str(self.starting_time)
        task_str += "\n" + "Finishing Time:\t" + str(self.finishing_time)
        task_str += "\n-------------"
        return task_str

class FleetManager:
    def __init__(self):
        self.airports = get_airports()

    def assign_task(self) -> Task:
        id_gen = str(random.randint(0,10000))
        airport = self.airports[random.randint(0, len(self.airports) - 1)]
        fleet = airport.fleets[random.randint(0, len(airport.fleets) - 1)]
        area = airport.areas[random.randint(0, len(airport.areas) - 1)]
        robot = fleet.robots[random.randint(0, len(fleet.robots) - 1)]
        task = robot.task_types[random.randint(0, len(robot.task_types) - 1)]
        content = str({
            "id_gen": id_gen,
            "airport": airport.sum(),
            "fleet": fleet.sum(),
            "area": area.sum(),
            "robot": robot.sum(),
            "task": task
        })
        
        task_id = hashlib.md5(content.encode()).hexdigest()

        return Task(
            id=task_id,
            airport=airport.sum(),
            fleet=fleet.sum(),
            area=area.sum(),
            robot=robot.sum(),
            task=task
        )


    def present(self):
        fm_str = "Fleet Manager" + "\n"
        for a in self.airports:
            fm_str += "\n" + a.present()
            fm_str += "\n--------------------"
        return fm_str

