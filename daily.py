import datetime
import flylat.scripts.getRoutes as getRoutes
import flylat.scripts.getCompanyData as getCompanyData


def main():
    print("Starting daily data collection scripts at", datetime.datetime.now(datetime.timezone.utc))
    getCompanyData.main()
    getRoutes.main()


if __name__ == "__main__":
    main()
