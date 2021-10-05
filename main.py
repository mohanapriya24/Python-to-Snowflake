import json
import csv
import pandas
import snowflake.connector
try:
    errorOccurred = open('errorRecord.txt', 'w')
    user = open(r'JSON\\userdetails.json', 'r')
    openmaster = open('CSV\\MasterFile.csv', 'r')
    master = openmaster.read()
    numberOfLinesInMaster = len(master.splitlines())
    #print("number of lines in master table",numberOfLinesInMaster)
    data = json.load(user)
    user.close()


    def run_query(giveconnection, givequery):
        cursor = giveconnection.cursor()
        cursor.execute(givequery)
        cursor.close()


    def insert_values(link_location, table_name):
        f = open(link_location, 'r')
        num = f.read()
        numoflines = len(num.splitlines())
        # print("number of lines in location table",numoflines)
        with open(link_location, 'r') as csvfiletoload:
            reader = csv.reader(csvfiletoload)
            next(reader)
            try:
                if numoflines > 1:
                    for i in range(numoflines - 1):
                        a = str(next(reader))
                        a = a.replace("[", "(")
                        a = a.replace("]", ");")
                        try:
                            query = "insert into " + table_name + " values" + a
                            # print(query)
                            insertfun = pandas.read_sql(query, connect)
                        except Exception:
                            error = "Line number ", i + 1, "is not inserted\n"
                            print(error)
                            # errorOccurred.write(error)
                            # continue

            except Exception as e:
                error = "Error occurred while inserting values\n" + str(e) + "\n"
                errorOccurred.write(error)


    def dimension_function(query):
        print(query)
        try:
            insertfun = pandas.read_sql(query, connect)
        except Exception as e:
            print(str(e))


    def select_function():
        # number of customers based on city
        query = "select count(c.custID) as number_of_customers_in_Chennai from CustomerDetails c join CustomerAddress ca on c.custID=ca.custID where ca.CITY='Chennai';"
        insertfun = pandas.read_sql(query, connect)
        print(insertfun)
        query = "select count(c.custID) as number_of_customers_in_Mumbai from CustomerDetails c join CustomerAddress ca on c.custID=ca.custID where ca.CITY='Mumbai';"
        insertfun = pandas.read_sql(query, connect)
        print(insertfun)
        query = "select count(c.custID) as number_of_customers_in_Banglore from CustomerDetails c join CustomerAddress ca on c.custID=ca.custID where ca.CITY='Banglore';"
        insertfun = pandas.read_sql(query, connect)
        print(insertfun)
        query = "select count(c.custID) as number_of_customers_in_Kolkata from CustomerDetails c join CustomerAddress ca on c.custID=ca.custID where ca.CITY='Kolkata';"
        insertfun = pandas.read_sql(query, connect)
        print(insertfun)

        # number of customers based on branch
        query = "select count(c.custID) as number_of_customers_in_Branch,b.IFSC from CustomerDetails c join AccountDetails a on (c.custID=a.custID) join BranchDetails b on(a.IFSC=b.IFSC) group by  b.IFSC;"
        insertfun = pandas.read_sql(query, connect)
        print(insertfun)
        query = "select count(c.custID) as Number_Of_Female_Customers,b.BranchName from CustomerDetails c join AccountDetails a on (c.custID=a.custID) join BranchDetails b on(a.IFSC=b.IFSC) where c.gender='F' group by  b.BranchName;"
        insertfun = pandas.read_sql(query, connect)
        print(insertfun)
        query = "select count(c.custID) as Number_Of_male_Customers,b.BranchName from CustomerDetails c join AccountDetails a on (c.custID=a.custID) join BranchDetails b on(a.IFSC=b.IFSC) where c.gender='M' group by  b.BranchName;"
        insertfun = pandas.read_sql(query, connect)
        print(insertfun)

        # number of ATM based on state
        query = "select count(ATMID) as number_of_ATM,CITY from ATMDetails group by CITY;"
        insertfun = pandas.read_sql(query, connect)
        print(insertfun)

        # number of ATM based on city
        query = "select count(ATMID) as number_of_ATM,STATE from ATMDetails group by STATE;"
        insertfun = pandas.read_sql(query, connect)
        print(insertfun)


    if __name__ == '__main__':
        try:
            connect = snowflake.connector.connect(user=data["user"],
                                                  password=data["password"],
                                                  account=data["account"],
                                                  )
            try:
                warehouseData = 'use warehouse {}'.format(data["warehouse"])
                run_query(connect, warehouseData)

                giveDatabase = 'use database {}'.format(data["database"])
                run_query(connect, giveDatabase)

                giveRole = 'use role {}'.format(data["role"])
                run_query(connect, giveRole)

                giveSchema = f'use schema {data["schema"]}'
                run_query(connect, giveSchema)
                try:
                    with open("CSV\\MasterFile.csv",
                              'r') as OpenThisFirst:
                        masterF = csv.reader(OpenThisFirst)
                        for line in range(numberOfLinesInMaster):
                            masterdata = next(masterF)
                            # print(masterdata)
                            location = str(masterdata[0])
                            table = str(masterdata[1])
                            # print("Location",location)
                            # print("table",table)
                            insert_values(location, table)
                            try:
                                if masterdata[2]:
                                    # print(masterdata[2])
                                    dimension_function(masterdata[2])
                            except:
                                continue
                        select_function()

                except FileNotFoundError:
                    errorOccurred.write("This csv file does not exist\n")
                except Exception as e:
                    error = str(e) + "\n"
                    errorOccurred.write(error)

            except Exception as e:
                error = str(e) + "\n"
                errorOccurred.write(error)
        except KeyError:
            error = "Given key is not found in the json file\n"
            errorOccurred.write(error)
        except ImportError as e:
            error = "There is some error in importing\n" + str(e) + "\n"
            errorOccurred.write(error)
        except Exception as e:
            error = str(e) + "\n"
            errorOccurred.write(error)

except FileNotFoundError:
    errorOccurred.write("This json file does not exist\n")
except ImportError as e:
    error = "There is some error in importing\n" + str(e) + "\n"
    errorOccurred.write(error)
except Exception as e:
    error = str(e) + "\n"
    errorOccurred.write(error)
finally:
    errorOccurred.close()
