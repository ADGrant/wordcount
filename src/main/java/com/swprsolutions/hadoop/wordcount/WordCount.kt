package com.swprsolutions.hadoop.wordcount

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import java.io.IOException
import java.util.*

object WordCount {
    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val conf = Configuration()
        val job = Job(conf, "wordcount")
        job.setJarByClass(WordCount::class.java)
        job.outputKeyClass = Text::class.java
        job.outputValueClass = IntWritable::class.java
        job.mapperClass = Map::class.java
        job.reducerClass = Reduce::class.java
        job.combinerClass = Reduce::class.java
        job.inputFormatClass = TextInputFormat::class.java
        job.outputFormatClass = TextOutputFormat::class.java
        FileInputFormat.addInputPath(job, Path(args[0]))
        FileOutputFormat.setOutputPath(job, Path(args[1]))
        job.waitForCompletion(true)
    }

    class Map : Mapper<LongWritable?, Text, Text?, IntWritable?>() {
        private val word = Text()
        @Throws(IOException::class, InterruptedException::class)
        public override fun map(key: LongWritable?, value: Text, context: Context) {
            val line = value.toString()
            val tokenizer = StringTokenizer(line)
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken())
                context.write(word, one)
            }
        }

        companion object {
            private val one = IntWritable(1)
        }
    }

    class Reduce : Reducer<Text?, IntWritable, Text?, IntWritable?>() {
        @Throws(IOException::class, InterruptedException::class)
        public override fun reduce(key: Text?, values: Iterable<IntWritable>, context: Context) {
            var sum = 0
            for (`val` in values) {
                sum += `val`.get()
            }
            context.write(key, IntWritable(sum))
        }
    }
}