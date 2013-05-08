import class TreeSet = "java.util.TreeSet"

def class ResultSet() =
  val results = TreeSet[String]()
  val sem = Semaphore(1)
  val limit = Ref(10000)
  
  def getLimit() = limit?
  def setLimit(-1) = signal
  def setLimit(x) = limit := x
  
  def size() = results.size()
  def clear() = results.clear()
  
  def contains(x) =
    sem.acquire() >>
    results.contains(x) > ret >
    sem.release() >>
    ret
  
  def popInternal(itr, true) =
    itr.next() > top >
    results.remove(top) >>
    top
  def popInternal(itr, false) =
    signal
  
  def pop() =
    sem.acquire() >>
    results.iterator() > itr >
    popInternal(itr, itr.hasNext()) >ret>
    sem.release() >>
    ret

  def addOne(x) =
    sem.acquire() >>
    results.add(WriteJSON(x)) >>
    sem.release()
  def add([]) = signal
  def add(x:xs) =
    (sem.acquire() >>
    results.add(WriteJSON(x)) >>
    sem.release()) |
    add(xs)

{-
  def add([]) = stop
  def add(x:xs) = 
    (
      (sem.acquire() >>
       (if results.size() <: getLimit() then results.add(WriteJSON(x)) else sem.release()) >>
       sem.release()
      ) ,
      (if results.size() <: getLimit() then add(xs) else stop)
    )
-}

  def toListInternal(itr) =
    -- ASSUMPTION: it is assumed that the semaphore is already acquired
    -- at this point.
    if itr.hasNext() then itr.next()>next> (next : toListInternal(itr))
    else []

  def toList() =
    sem.acquire() >>
    results.iterator() > itr >
    toListInternal(itr) > ret > 
    sem.release() >>
    ret
  
  def screenNamesInternal(itr) =
    if itr.hasNext() then itr.next() > next >
                          ReadJSON(next).screen_name > ret >
                          (ret : screenNamesInternal(itr))
                     else []
    
  def screenNames() =
    sem.acquire() >>
    results.iterator() > itr >
    screenNamesInternal(itr) > ret >
    sem.release() >>
    ret
    
  def printInternal([]) = signal
  def printInternal(x:xs) =
    Println(x) >> printInternal(xs)
  def printAll() =
    toList() >list> printInternal(list)
stop

def class QueryManager(workers) =
  
  val results = ResultSet()
  val processedSet = ResultSet()
  val sem = Semaphore(1)
  
  def getResults() = results
  
  def setLimit(x) = results.setLimit(x)
  
  def queryInternal([], q) = signal
  def queryInternal(x:xs, q) =
    ReadJSON(HTTP(machineName(x, q)).get()) > obj >
    results.add(obj) |
    queryInternal(xs, q)

  def machineName(x, q) =
    "http://" + x + ".cs.utexas.edu:8080?query=" + q

  def query(q) =
    results.clear() >>
    q.replace(" ", "+") > queryString >
    Println(queryString) >>
    queryInternal(workers, queryString)

  def print(signal) = signal
  def print(x) =
    processedSet.size() > oldSize >
    processedSet.addOne(x) >>
    processedSet.size() > newSize >
    Ift(newSize /= oldSize && newSize <= results.getLimit()) >> Println(x) ; signal

  def getFollowers(username) =
    val queryString = "select u.screen_name from Users u, Followers f where f.screen_name = '" + username + "' and u.screen_name = f.follower_id"
    query(queryString)
  
  
  
  

  def display() =
    --Rwait(10) >>
    results.pop() > ret >
    print(ret) >> 
    display()
    --Ift(ret /= signal) >> Println(ret) >> display(); display()

stop

{-
def getFollowers([], out_set) = signal
def getFollowers(x:xs, out_set) =
  val queryString = "select u.screen_name from Users u, Followers f where f.screen_name = '" + x + "' and u.screen_name = f.follower_id"
  query(workers, queryString.replace(" ", "+"), out_set) >> getFollowers(xs, out_set) ; getFollowers(xs, out_set)
-}

def getSecondLevelResults(username, attribute_selector) =
  val queryString = "select u.screen_name from Users u, Followers f where f.screen_name = '" + username + "' and u.screen_name = f.follower_id AND " + attribute_selector
  Println(queryString)

def getSecondLevelFollowersInternal(results, attribute_selector) =
  Rwait(1000) >> results.pop() > top >
  Iff(top = signal) >> ReadJSON(top).screen_name > username >
                       getSecondLevelResults(username, attribute_selector) >>
                       getSecondLevelFollowersInternal(results, attribute_selector) 
  ; getSecondLevelFollowersInternal(results, attribute_selector)

def handleNestedFollowers(username, attribute_selector) =
  val qManager = QueryManager(["raisinets", "chastity", "diligence", "patience", "aero", "airheads", "humility", "twix", "angry-goat", "dots", "dubble-bubble", "candy-corn", "turtles", "adler", "gummi-bears", "fun-dip", "heath", "wrath", "astral-badger", "envy", "gluttony", "greed", "kindness", "hasselblad", "inskeep", "leica"])
  qManager.getFollowers(username) | getSecondLevelFollowersInternal(qManager.getResults(), attribute_selector)

def run() =
  val qManager = QueryManager(["raisinets", "chastity", "diligence", "patience", "aero", "airheads", "humility", "twix", "angry-goat", "dots", "dubble-bubble", "candy-corn", "turtles", "adler", "gummi-bears", "fun-dip", "heath", "wrath", "astral-badger", "envy", "gluttony", "greed", "kindness", "hasselblad", "inskeep", "leica"])
  Prompt("Enter query string") > queryString >
  HTTP("http://roadkill.cs.utexas.edu:8080?query=" + queryString.replace(" ", "+")).get() > sqlJSON >
  Println("sqlJSON = " + sqlJSON) >>
  ReadJSON(sqlJSON) > sqlParsedJson >
  Println("limit = " + sqlParsedJson.limit) >>
  Println("queryString = " + sqlParsedJson.queryString) >>
  qManager.setLimit(sqlParsedJson.limit) >>
  (qManager.query(sqlParsedJson.queryString.replace(" ", "+")) | qManager.display())

def printResults(result_set) =
  Println(result_set.size()) >>
  result_set.printAll() {->>
  result_set.screenNames() > usersList >
  result_set.clear() >>
  getFollowers(usersList, result_set) >>
  Println(result_set.size()) >>
  result_set.screenNames()-}

--run()-- >> printResults(qManager.getResults())  ; printResults(qManager.getResults())
handleNestedFollowers("mitsiddharth", " u.location like '%austin%' and u.name like '%hemanth%' ")


