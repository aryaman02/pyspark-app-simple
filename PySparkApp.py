from pyspark.sql import SparkSession

def filterHelper(playerStats: str) -> bool:
    statsArr = playerStats.split(",")
    highScore = statsArr[7]

    if (highScore == "-"):
        return False
    else:
        strBuilder = ""
        for i in range(len(highScore)):
            if highScore[i].isdigit():
                strBuilder += highScore[i]
        if strBuilder == "":
            return False

        realHS = int(strBuilder)

        if realHS >= 90:
            return True
        else:
            return False

def runsHelper(playerStats: str) -> int:
    statsArr = playerStats.split(",")
    return int(statsArr[6])

def ballsFacedHelper(playerStats: str) -> int:
    statsArr = playerStats.split(",")
    return int(statsArr[9])

def selectPlayersBetterSRHelper(playerStats: str, filterSRCond: float) -> bool:
    statsArr = playerStats.split(",")
    playerSR = float(statsArr[10])

    return (playerSR > filterSRCond)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    spark = SparkSession.builder.appName("PySparkApp").getOrCreate()
    sc = spark.sparkContext

    cricDataRDD = sc.textFile("/Users/aryaman/spark-apps/pyspark-app-simple/t20.csv")

    prolificPlayersRDD = cricDataRDD.filter(lambda x: filterHelper(x))

    totalRunsScored = prolificPlayersRDD.map(lambda x: runsHelper(x)).sum()

    totalBallsFaced = prolificPlayersRDD.map(lambda x: ballsFacedHelper(x)).sum()

    avgSRPlayers = 100.0 * (totalRunsScored / totalBallsFaced)

    bestPlayersRDD = prolificPlayersRDD.filter(lambda x: selectPlayersBetterSRHelper(x, avgSRPlayers))

    spark.stop()

