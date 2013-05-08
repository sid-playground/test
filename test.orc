import class TreeSet = "java.util.TreeSet"

def class ResultSet() =
  val results = TreeSet[String]()
  val sem = Semaphore(1)
  val limit = Ref(10000)
  
  def getLimit() = limit?
  def setLimit(-1) = signal
  def setLimit(x) = limit := x
  
  def size() = results.size()
  def clear() = results.clear()
  
  def contains(x) =
    sem.acquire() >>
    results.contains(x) > ret >
    sem.release() >>
    ret
  
  def popInternal(itr, true) =
    itr.next() > top >
    results.remove(top) >>
    top
  def popInternal(itr, false) =
    signal
  
  def pop() =
    sem.acquire() >>
    results.iterator() > itr >
    popInternal(itr, itr.hasNext()) >ret>
    sem.release() >>
    ret

  def addOne(x) =
    sem.acquire() >>
    results.add(WriteJSON(x)) >>
    sem.release()
  def add([]) = signal
  def add(x:xs) =
    (sem.acquire() >>
    results.add(WriteJSON(x)) >>
    sem.release()) |
    add(xs)


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
  val processedSet = ResultSet()
  val sem = Semaphore(1)
  
  def getResults() = results
  
  def setLimit(x) = results.setLimit(x)
  
  def queryInternal([], q) = signal
  def queryInternal(x:xs, q) =
    ReadJSON(HTTP(machineName(x, q)).get()) > obj >
    results.add(obj) |
    queryInternal(xs, q)

  def machineName(x, q) =
    "http://" + x + ".cs.utexas.edu:8080?query=" + q

  def query(q) =
    results.clear() >>
    q.replace(" ", "+") > queryString >
    --Println(queryString) >>
    queryInternal(workers, queryString)

  def print(signal) = signal
  def print(x) =
    processedSet.size() > oldSize >
    processedSet.addOne(x) >>
    processedSet.size() > newSize >
    Ift(newSize /= oldSize && newSize <= results.getLimit()) >> Println(x) ; signal

  def getFollowers(username) =
    val queryString = "select u.screen_name from Users u, Followers f where f.screen_name = '" + username + "' and u.screen_name = f.follower_id"
    query(queryString)
    
  def executeQuery(0, sqlParsedJson) = 
    query(sqlParsedJson.queryString.replace(" ", "+")) | display()

  def executeQuery(1, sqlParsedJson) = 
    handleNestedFollowers(sqlParsedJson.root_username, sqlParsedJson.attribute_selector, sqlParsedJson.limit)

  def display() =
    Rwait(100) >>
    results.pop() > ret >
    print(ret) >> 
    display()

stop

def getSecondLevelResults(username, attribute_selector, limit) =
  val queryString = "select u.screen_name from Users u, Followers f where f.screen_name = '" + username + "' and u.screen_name = f.follower_id " + attribute_selector
  val qManager = QueryManager(["raisinets", "chastity", "diligence", "patience", "aero", "airheads", "humility", "twix", "angry-goat", "dots", "dubble-bubble", "candy-corn", "turtles", "adler", "gummi-bears", "fun-dip", "heath", "wrath", "astral-badger", "envy", "gluttony", "greed", "kindness", "hasselblad", "inskeep", "leica"])
  Println("setting limit = " + limit) >> qManager.setLimit(limit) >> (qManager.query(queryString) | qManager.display())

def getSecondLevelFollowersInternal(results, attribute_selector, limit) =
  Rwait(500) >> results.pop() > top >
  Iff(top = signal) >> ReadJSON(top).screen_name > username >
                       getSecondLevelResults(username, attribute_selector, limit) >>
                       getSecondLevelFollowersInternal(results, attribute_selector, limit) 
  ; getSecondLevelFollowersInternal(results, attribute_selector, limit)

def handleNestedFollowers(username, attribute_selector, limit) =
  val qManager = QueryManager(["raisinets", "chastity", "diligence", "patience", "aero", "airheads", "humility", "twix", "angry-goat", "dots", "dubble-bubble", "candy-corn", "turtles", "adler", "gummi-bears", "fun-dip", "heath", "wrath", "astral-badger", "envy", "gluttony", "greed", "kindness", "hasselblad", "inskeep", "leica"])
  qManager.setLimit(limit) >>
  (qManager.getFollowers(username) | getSecondLevelFollowersInternal(qManager.getResults(), attribute_selector, limit))



def run() =
  val qManager = QueryManager(["raisinets", "chastity", "diligence", "patience", "aero", "airheads", "humility", "twix", "angry-goat", "dots", "dubble-bubble", "candy-corn", "turtles", "adler", "gummi-bears", "fun-dip", "heath", "wrath", "astral-badger", "envy", "gluttony", "greed", "kindness", "hasselblad", "inskeep", "leica"])
  Prompt("Enter query string") > queryString >
  HTTP("http://roadkill.cs.utexas.edu:8080?query=" + queryString.replace(" ", "+")).get() > sqlJSON >
  Println("sqlJSON = " + sqlJSON) >>
  ReadJSON(sqlJSON) > sqlParsedJson >
  Println("limit = " + sqlParsedJson.limit) >>
  Println("queryString = " + sqlParsedJson.queryString) >>
  Println("attribute_selector = " + sqlParsedJson.attribute_selector) >>
  Println("root_user = " + sqlParsedJson.root_username) >>
  qManager.setLimit(sqlParsedJson.limit) >>
  qManager.executeQuery(sqlParsedJson.nested_followers, sqlParsedJson)
  --(qManager.query(sqlParsedJson.queryString.replace(" ", "+")) | qManager.display())

def printResults(result_set) =
  Println(result_set.size()) >>
  result_set.printAll() {->>
  result_set.screenNames() > usersList >
  result_set.clear() >>
  getFollowers(usersList, result_set) >>
  Println(result_set.size()) >>
  result_set.screenNames()-}

run()-- >> printResults(qManager.getResults())  ; printResults(qManager.getResults())
--handleNestedFollowers("mitsiddharth", " u.location like '%chennai%' ")



