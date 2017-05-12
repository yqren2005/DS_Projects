/**
  * Created by yren on 5/11/2017.
  */

import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

val decoder = Codec.UTF8.decoder.onMalformedInput(CodingErrorAction.IGNORE)

val lines = Source.fromFile("war_and_peace.txt")(decoder).getLines()

val counts = lines.map(line => line.replaceAll("[^a-zA-Z0-9\\s]", ""))
                  .flatMap(line => line.split("\\s+"))
                  .filter(_.nonEmpty)
                  .toSeq
                  .groupBy(_.toLowerCase)
                  .mapValues(_.length)

counts.toSeq.sortBy(_._2).reverse.take(20).foreach(println)