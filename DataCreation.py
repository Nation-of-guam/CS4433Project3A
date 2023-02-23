import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
# Set K value & generate points


def people_large():
    size = 1000000
    seedsx = np.random.randint(0, 10000, size=size)
    seedsy = np.random.randint(0, 10000, size=size)

    df = pd.DataFrame()
    df["id"] = range(0,size)
    df["x"] = seedsx
    df["y"] = seedsy

    name = "PEOPLE-large.csv"
    df.to_csv(name, index=False, header=True)
    return
def infected_small():
    df = pd.DataFrame()
    name = "PEOPLE-large.csv"
    try:
        df = pd.read_csv(name)
    except:
        people_large()
        infected_small()

    uninfected, infected = train_test_split(df, test_size=.01)

    uninfected["INFECTED"] = "no"
    infected["INFECTED"] = "yes"
    someInfected = infected.append(uninfected)


    name = "INFECTED-small.csv"
    infected.to_csv(name, index=False, header=True)
    name = "PEOPLE-SOME-INFECTED-large.csv"
    someInfected.to_csv(name, index=False, header=True)
    return
people_large()
infected_small()