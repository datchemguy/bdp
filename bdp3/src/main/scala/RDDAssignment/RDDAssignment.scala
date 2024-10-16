package RDDAssignment

import java.util.UUID
import java.math.BigInteger
import java.security.MessageDigest
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import utils.{Commit, File, Stats, User}

object RDDAssignment {
  private def repo(commit: Commit): String = commit.url.split("/").apply(5)
  private def fullrepo(commit: Commit): String = {
    val arr = commit.url.split("/")
    arr(4) + "/" + arr(5)
  }
  private def add(s1: Stats, s2: Stats): Stats = Stats(s1.total + s2.total, s1.additions + s2.additions, s1.deletions + s2.deletions)
  private def stat(op: Option[Stats]): Stats = op.getOrElse(Stats(0, 0, 0))

  /**
    * Reductions are often used in data processing in order to gather more useful data out of raw data. In this case
    * we want to know how many commits a given RDD contains.
    *
    * @param commits RDD containing commit data.
    * @return Long indicating the number of commits in the given RDD.
    */
  def assignment_1(commits: RDD[Commit]): Long = commits.count

  /**
    * We want to know how often programming languages are used in committed files. We want you to return an RDD containing Tuples
    * of the used file extension, combined with the number of occurrences. If no filename or file extension is used we
    * assume the language to be 'unknown'.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing tuples indicating the programming language (extension) and number of occurrences.
    */
  def assignment_2(commits: RDD[Commit]): RDD[(String, Long)] = commits.flatMap(_.files)
    .map(_.filename.getOrElse(""))
    .groupBy(x => if(x.contains('.')) x.substring(x.lastIndexOf('.')+1) else "unknown")
    .mapValues(_.size.toLong)

  /**
    * Competitive users on GitHub might be interested in their ranking in the number of commits. We want you to return an
    * RDD containing Tuples of the rank (zero indexed) of a commit author, a commit author's name and the number of
    * commits made by the commit author. As in general with performance rankings, a higher performance means a better
    * ranking (0 = best). In case of a tie, the lexicographical ordering of the usernames should be used to break the
    * tie.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing the rank, the name and the total number of commits for every author, in the ordered fashion.
    */
  def assignment_3(commits: RDD[Commit]): RDD[(Long, String, Long)] = commits.groupBy(_.commit.author.name)
    .mapValues(_.size.toLong)
    .sortBy(x => (-x._2, x._1.toLowerCase))
    .zipWithIndex
    .map(x => (x._2, x._1._1, x._1._2))

  /**
    * Some users are interested in seeing an overall contribution of all their work. For this exercise we want an RDD that
    * contains the committer's name and the total number of their commit statistics. As stats are Optional, missing Stats cases should be
    * handled as "Stats(0, 0, 0)".
    *
    * Note that if a user is given that is not in the dataset, then the user's name should not occur in
    * the resulting RDD.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing committer names and an aggregation of the committers Stats.
    */
  def assignment_4(commits: RDD[Commit], users: List[String]): RDD[(String, Stats)] = {
    val uset = users.toSet
    commits.filter(x => uset.contains(x.commit.committer.name))
      .groupBy(_.commit.committer.name)
      .mapValues(_.map(x => stat(x.stats)).reduce(add))
  }


  /**
    * There are different types of people: those who own repositories, and those who make commits. Although Git blame command is
    * excellent in finding these types of people, we want to do it in Spark. As the output, we require an RDD containing the
    * names of commit authors and repository owners that have either exclusively committed to repositories, or
    * exclusively own repositories in the given commits RDD.
    *
    * Note that the repository owner is contained within GitHub URLs.
    *
    * @param commits RDD containing commit data.
    * @return RDD of Strings representing the usernames that have either only committed to repositories or only own
    *         repositories.
    */
  def assignment_5(commits: RDD[Commit]): RDD[String] = {
    val owners = commits.map(_.url.split('/').apply(4))
    val committers = commits.map(_.commit.author.name)
    ((owners subtract committers) union (committers subtract owners)).distinct
  }

  /**
    * Sometimes developers make mistakes and sometimes they make many many of them. One way of observing mistakes in commits is by
    * looking at so-called revert commits. We define a 'revert streak' as the number of times `Revert` occurs
    * in a commit message. Note that for a commit to be eligible for a 'revert streak', its message must start with `Revert`.
    * As an example: `Revert "Revert ...` would be a revert streak of 2, whilst `Oops, Revert Revert little mistake`
    * would not be a 'revert streak' at all.
    *
    * We require an RDD containing Tuples of the username of a commit author and a Tuple containing
    * the length of the longest 'revert streak' of a user and how often this streak has occurred.
    * Note that we are only interested in the longest commit streak of each author (and its frequency).
    *
    * @param commits RDD containing commit data.
    * @return RDD of Tuples containing a commit author's name and a Tuple which contains the length of the longest
    *         'revert streak' as well its frequency.
    */
  def assignment_6(commits: RDD[Commit]): RDD[(String, (Int, Int))] = {
    def streak(s: String): Int = if(!s.startsWith("Revert")) 0 else s.sliding(6).count(_ == "Revert")
    commits.map(x => (x.commit.author.name, (streak(x.commit.message), 1)))
      .filter(_._2._1 != 0)
      .reduceByKey((x,y) => if(y._1 > x._1) y else (x._1, x._2 + 1))
  }


  /**
    * !!! NOTE THAT FROM THIS EXERCISE ON (INCLUSIVE), EXPENSIVE FUNCTIONS LIKE groupBy ARE NO LONGER ALLOWED TO BE USED !!
    *
    * We want to know the number of commits that have been made to each repository contained in the given RDD. Besides the
    * number of commits, we also want to know the unique committers that contributed to each of these repositories.
    *
    * In real life these wide dependency functions are performance killers, but luckily there are better performing alternatives!
    * The automatic graders will check the computation history of the returned RDDs.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing Tuples with the repository name, the number of commits made to the repository as
    *         well as the names of the unique committers to this repository.
    */
  def assignment_7(commits: RDD[Commit]): RDD[(String, Long, Iterable[String])] = commits
    .map(x => (repo(x), (1L, Set(x.commit.committer.name))))
    .reduceByKey((x, y) => (x._1 + y._1, x._2.union(y._2)))
    .map(x => (x._1, x._2._1, x._2._2))

  /**
    * Return an RDD of Tuples containing the repository name and all the files that are contained in this repository.
    * Note that the file names must be unique, so if a file occurs multiple times (for example, due to removal, or new
    * addition), the newest File object must be returned. As the filenames are an `Option[String]`, discard the
    * files that do not have a filename.
    *
    * To reiterate, expensive functions such as groupBy are not allowed.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing the files in each repository as described above.
    */
  def assignment_8(commits: RDD[Commit]): RDD[(String, Iterable[File])] = commits
    .map(x => ((repo(x), x.commit.committer.date), x.files.filter(_.filename.nonEmpty)))
    .flatMapValues(x => x)
    .map(x => (x._1._1, (x._2, x._1._2)))
    .sortBy(x => (x._2._1.filename.get, -x._2._2.getTime))
    .map(x => (x._1, List(x._2._1)))
    .reduceByKey((x,y) => {
      val next = y.head
      if(x.head.filename.get == next.filename.get) x else next :: x
    }).mapValues(_.toIterable)


  /**
    * For this assignment you are asked to find all the files of a single repository. This is in order to create an
    * overview of each file by creating a Tuple containing the file name, all corresponding commit SHA's,
    * as well as a Stat object representing all the changes made to the file.
    *
    * To reiterate, expensive functions such as groupBy are not allowed.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing Tuples representing a file name, its corresponding commit SHA's and a Stats object
    *         representing the total aggregation of changes for a file.
    */
  def assignment_9(commits: RDD[Commit], repository: String): RDD[(String, Seq[String], Stats)] = commits
    .filter(repo(_) == repository)
    .map(x => (x.parents.map(_.sha), x))
    .flatMapValues(_.files)
    .filter(_._2.filename.nonEmpty)
    .map(x => (x._2.filename.get, (x._1, Stats(x._2.changes, x._2.additions, x._2.deletions))))
    .reduceByKey((x, y) => (x._1 ::: y._1, add(x._2, y._2)))
    .map(x => (x._1, x._2._1, x._2._2))

  /**
    * We want to generate an overview of the work done by a user per repository. For this we want an RDD containing
    * Tuples with the committer's name, the repository name and a `Stats` object containing the
    * total number of additions, deletions and total contribution to this repository.
    * Note that since Stats are optional, the required type is Option[Stat].
    *
    * To reiterate, expensive functions such as groupBy are not allowed.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing Tuples of the committer's name, the repository name and an `Option[Stat]` object representing additions,
    *         deletions and the total contribution to this repository by this committer.
    */
  def assignment_10(commits: RDD[Commit]): RDD[(String, String, Option[Stats])] = commits
    .map(x => ((x.commit.committer.name, repo(x)), x.stats))
    .reduceByKey((x, y) => if(x.isEmpty && y.isEmpty) None else Some(add(stat(x), stat(y))))
    .map(x => (x._1._1, x._1._2, x._2))


  /**
    * Hashing function that computes the md5 hash of a String and returns a Long, representing the most significant bits of the hashed string.
    * It acts as a hashing function for repository name and username.
    *
    * @param s String to be hashed, consecutively mapped to a Long.
    * @return Long representing the MSB of the hashed input String.
    */
  private def md5HashString(s: String): Long = {
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(s.getBytes)
    val bigInt = new BigInteger(1, digest)
    val hashedString = bigInt.toString(16)
    UUID.nameUUIDFromBytes(hashedString.getBytes()).getMostSignificantBits
  }

  /**
    * Create a bi-directional graph from committer to repositories. Use the `md5HashString` function above to create unique
    * identifiers for the creation of the graph.
    *
    * Spark's GraphX library is actually used in the real world for algorithms like PageRank, Hubs and Authorities, clique finding, etc.
    * However, this is out of the scope of this course and thus, we will not go into further detail.
    *
    * We expect a node for each repository and each committer (based on committer name).
    * We expect an edge from each committer to the repositories that they have committed to.
    *
    * Look into the documentation of Graph and Edge before starting with this exercise.
    * Your vertices must contain information about the type of node: a 'developer' or a 'repository' node.
    * Edges must only exist between repositories and committers.
    *
    * To reiterate, expensive functions such as groupBy are not allowed.
    *
    * @param commits RDD containing commit data.
    * @return Graph representation of the commits as described above.
    */
  def assignment_11(commits: RDD[Commit]): Graph[(String, String), String] = {
    val verts = commits.map(x => (md5HashString(fullrepo(x) + "r"), ("repository", fullrepo(x))))
      .union(commits.map(x => (md5HashString(x.commit.committer.name + "d"), ("developer", x.commit.committer.name))))
    val edges = commits.map(x => Edge(md5HashString(x.commit.committer.name + "d"), md5HashString(fullrepo(x) + "r"), "edge")).distinct
    Graph.apply(verts, edges)
  }
}
