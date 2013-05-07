import class TreeSet = "java.util.TreeSet"

def class ResultSet() =
  val results = TreeSet[String]()
  val sem = Semaphore(1)
  val limit = Ref(10000)
  
  def getLimit() = limit?
  def setLimit(x) = limit := x
  
  def size() = results.size()
  def clear() = results.clear()
  def add([]) = stop
  def add(x:xs) = 
    (
      (sem.acquire() >>
       (if results.size() <: getLimit() then results.add(WriteJSON(x)) else sem.release() >> stop) >>
       sem.release()
      ) ,
      add(xs)
    )
  def toListInternal(itr) =
    -- ASSUMPTION: it is assumed that the semaphore is already acquired
    -- at this point.
    if itr.hasNext() then itr.next()>next> (next : toListInternal(itr))
    else []

  def toList() =
    sem.acquire() >>
    results.iterator() > itr >
    toListInternal(itr) > ret > 
    sem.release() >>
    ret
  
  def screenNamesInternal(itr) =
    if itr.hasNext() then itr.next() > next >
                          ReadJSON(next).screen_name > ret >
                          (ret : screenNamesInternal(itr))
                     else []
    
  def screenNames() =
    sem.acquire() >>
    results.iterator() > itr >
    screenNamesInternal(itr) > ret >
    sem.release() >>
    ret
    
  def printInternal([]) = signal
  def printInternal(x:xs) =
    Println(x) >> printInternal(xs)
  def printAll() =
    toList() >list> printInternal(list)
stop

def class QueryManager(workers) =
  
  val results = ResultSet()
  val offset = Ref(0)
  val limit = 1
  
  def getResults() = results
  
  def setLimit(x) = results.setLimit(x)
  
  def queryInternal([], q) = signal
  def queryInternal(x:xs, q) =
    (
      ReadJSON(HTTP(machineName(x, q)).get()) > obj >
      results.add(obj) ,
      queryInternal(xs, q)
    )

  def machineName(x, q) =
    "http://" + x + ".cs.utexas.edu:8080?query=" + q

  def query(queryString) =
    results.clear() >>
    Println(queryString) >>
    queryInternal(workers, queryString)

stop

{-
def getFollowers([], out_set) = signal
def getFollowers(x:xs, out_set) =
  val queryString = "select u.screen_name from Users u, Followers f where f.screen_name = '" + x + "' and u.screen_name = f.follower_id"
  query(workers, queryString.replace(" ", "+"), out_set) >> getFollowers(xs, out_set) ; getFollowers(xs, out_set)
-}

val qManager = QueryManager(["raisinets", "chastity", "diligence", "patience", "aero", "airheads", "humility", "twix", "angry-goat", "dots", "dubble-bubble", "candy-corn", "turtles", "adler", "gummi-bears", "fun-dip", "heath", "wrath", "astral-badger", "envy", "gluttony", "greed", "kindness", "hasselblad", "inskeep", "leica"])

def run() =
  Prompt("Enter query string") > queryString >
  HTTP("http://roadkill.cs.utexas.edu:8080?query=" + queryString.replace(" ", "+")).get() > sqlJSON >
  Println("sqlJSON = " + sqlJSON) >>
  ReadJSON(sqlJSON) > sqlParsedJson >
  Println("limit = " + sqlParsedJson.limit) >>
  Println("queryString = " + sqlParsedJson.queryString) >>
  qManager.setLimit(sqlParsedJson.limit) >>
  qManager.query(sqlParsedJson.queryString.replace(" ", "+"))

def printResults(result_set) =
  Println(result_set.size()) >>
  result_set.printAll() {->>
  result_set.screenNames() > usersList >
  result_set.clear() >>
  getFollowers(usersList, result_set) >>
  Println(result_set.size()) >>
  result_set.screenNames()-}

--val r = ResultSet()
--val r2 = ResultSet()
run() >> printResults(qManager.getResults())  ; printResults(qManager.getResults())

