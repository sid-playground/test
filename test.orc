import class TreeSet = "java.util.TreeSet"

def class ResultSet() =
  val results = TreeSet[String]()
  val sem = Semaphore(1)
  def add([]) = stop
  def size() = Println("here") >> results.size()
  def clear() = results.clear()
  def add(x:xs) = 
    (
      (sem.acquire() >>
      results.add(WriteJSON(x)) >>
      sem.release()) ,
      add(xs)
    )
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

def machineName(x, q) =
  "http://" + x + ".cs.utexas.edu:8080?query=" + q

def query([], q, out_set) = signal
def query(x:xs, q, out_set) =
  (
    ReadJSON(HTTP(machineName(x, q)).get()) > obj >
    out_set.add(obj) ,
    query(xs, q, out_set)
  )

val workers = ["raisinets", "chastity", "diligence", "patience", "aero"]
val query_servers = ["candy-corn"]

def getFollowers([], out_set) = signal
def getFollowers(x:xs, out_set) =
  val queryString = "select u.screen_name from Users u, Followers f where f.screen_name = '" + x + "' and u.screen_name = f.follower_id"
  query(workers, queryString.replace(" ", "+"), out_set) >> getFollowers(xs, out_set) ; getFollowers(xs, out_set)

def run(out_set) =
  Prompt("Enter query string") > queryString >
  HTTP("http://roadkill.cs.utexas.edu:8080?query=" + queryString.replace(" ", "+")).get() > sqlString >
  query(workers, sqlString.replace(" ", "+"), out_set)

def printResults(result_set) =
  Println(result_set.size()) >>
  result_set.printAll() >>
  result_set.screenNames() > usersList >
  result_set.clear() >>
  getFollowers(usersList, result_set) >>
  Println(result_set.size()) >>
  result_set.screenNames()

val r = ResultSet()
run(r) >> printResults(r)  ; printResults(r)



