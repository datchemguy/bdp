package dataset

import dataset.util.Commit.Commit

import java.text.SimpleDateFormat
import java.util.SimpleTimeZone

/**
 * Use your knowledge of functional programming to complete the following functions.
 * You are recommended to use library functions when possible.
 *
 * The data is provided as a list of `Commit`s. This case class can be found in util/Commit.scala.
 * When asked for dates, use the `commit.commit.committer.date` field.
 *
 * This part is worth 40 points.
 */
object Dataset {
  private def repoOf(commit: Commit): String = {
    val parts = commit.url.split("https://api.github.com/repos/|/")
    parts.apply(1) + "/" + parts.apply(2)
  }
  private def hour(commit: Commit): Int = {
    val form = new SimpleDateFormat("H")
    form.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"))
    form.format(commit.commit.committer.date).toInt
  }

  /** Q23 (4p)
   * For the commits that are accompanied with stats data, compute the average of their additions.
   * You can assume a positive amount of usable commits is present in the data.
   *
   * @param input the list of commits to process.
   * @return the average amount of additions in the commits that have stats data.
   */
  def avgAdditions(input: List[Commit]): Int = {
    val good = input.filter(_.stats.nonEmpty).map(_.stats.get.additions)
    good.sum / good.length
  }

  /** Q24 (4p)
   * Find the hour of day (in 24h notation, UTC time) during which the most javascript (.js) files are changed in commits.
   * The hour 00:00-00:59 is hour 0, 14:00-14:59 is hour 14, etc.
   * NB!filename of a file is always defined.
   * Hint: for the time, use `SimpleDateFormat` and `SimpleTimeZone`.
   *
   * @param input list of commits to process.
   * @return the hour and the amount of files changed during this hour.
   */
  def jsTime(input: List[Commit]): (Int, Int) = input.groupBy(hour)
      .mapValues(x => x.flatMap(_.files).count(f => f.filename.get.endsWith(".js")))
      .maxBy(_._2)


  /** Q25 (5p)
   * For a given repository, output the name and amount of commits for the person
   * with the most commits to this repository.
   * For the name, use `commit.commit.author.name`.
   *
   * @param input the list of commits to process.
   * @param repo  the repository name to consider.
   * @return the name and amount of commits for the top committer.
   */
  def topCommitter(input: List[Commit], repo: String): (String, Int) = {
    input.filter(repoOf(_) == repo).groupBy(_.commit.author.name)
      .mapValues(_.length).maxBy(_._2)
  }

  /** Q26 (9p)
   * For each repository, output the name and the amount of commits that were made to this repository in 2019 only.
   * Leave out all repositories that had no activity this year.
   *
   * @param input the list of commits to process.
   * @return a map that maps the repo name to the amount of commits.
   *
   *         Example output:
   *         Map("KosDP1987/students" -> 1, "giahh263/HQWord" -> 2)
   */
  def commitsPerRepo(input: List[Commit]): Map[String, Int] = input.groupBy(repoOf)
    .mapValues(_.count(x => new SimpleDateFormat("yyyy").format(x.commit.committer.date) == "2019"))
    .filter(_._2 != 0)


  /** Q27 (9p)
   * Derive the 5 file types that appear most frequent in the commit logs.
   * NB!filename of a file is always defined.
   * @param input the list of commits to process.
   * @return 5 tuples containing the file extension and frequency of the most frequently appeared file types, ordered descendingly.
   */
  def topFileFormats(input: List[Commit]): List[(String, Int)] = input.flatMap(_.files)
    .map(_.filename.get).groupBy(x => x.substring(x.lastIndexOf('.')+1)).mapValues(_.length)
    .toList.sortBy(-_._2).take(5)


  /** Q28 (9p)
   *
   * A day has different parts:
   * morning 5 am to 12 pm (noon)
   * afternoon 12 pm to 5 pm.
   * evening 5 pm to 9 pm.
   * night 9 pm to 4 am.
   *
   * Which part of the day was the most productive in terms of commits ?
   * Return a tuple with the part of the day and the number of commits
   *
   * Hint: for the time, use `SimpleDateFormat` and `SimpleTimeZone`.
   */
  def mostProductivePart(input: List[Commit]): (String, Int) = input.groupBy(x => {
    val h = hour(x)
    if(h < 5) "night" else if(h < 12) "morning" else if(h < 17) "afternoon" else if(h < 21) "evening" else "night"
  }).mapValues(_.length).maxBy(_._2)

}
