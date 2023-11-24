package top.binlab.cassandra.handler


import org.apache.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Option, Options, ParseException}

import java.io.PrintWriter

trait TraitCommandLineHandler {
  private var options: Options = _
  private var formatter: HelpFormatter = _
  private var jobName: String = _


  def initOptions(jobName: String): Unit = {
    options = new Options()
    formatter = new HelpFormatter()
    this.jobName = jobName
  }

  def printHelpWithError(e: ParseException): Unit = {
    println(e.getMessage())
    printHelp()
  }

  def printHelp(): Unit = {
    formatter.printHelp(jobName, options)
  }

  def printUsage(): Unit = {
    val pw = new PrintWriter(System.out);
    formatter.printUsage(pw, HelpFormatter.DEFAULT_WIDTH, jobName, options)
    pw.flush()
  }

  private def addOptionInternal(opt: String, hasArg: Boolean, description: String): Option = {
    new Option(opt, hasArg, description)
  }

  private def addOptionInternal(opt: String, longOpt: String, hasArg: Boolean, description: String): Option = {
    new Option(opt, longOpt, hasArg, description)
  }


  def addOption(opt: String, hasArg: Boolean, description: String): Option = {
    val option = addOptionInternal(opt, hasArg, description)
    options.addOption(option)
    option
  }

  def addOption(opt: String, longOpt: String, hasArg: Boolean, description: String): Option = {
    val option = addOptionInternal(opt, longOpt, hasArg, description)
    options.addOption(option)
    option
  }

  def addRequireOption(opt: String, hasArg: Boolean, description: String): Option = {
    val option = addOptionInternal(opt, hasArg, description)
    option.setRequired(true)
    options.addOption(option)
    option
  }

  def addRequireOption(opt: String, longOpt: String, hasArg: Boolean, description: String): Option = {
    val option = addOptionInternal(opt, longOpt, hasArg, description)
    option.setRequired(true)
    options.addOption(option)
    option
  }

  def parseCommandLine(args: Array[String]): CommandLine = {
    var cmd: CommandLine = null
    try {
      val parser = new DefaultParser
      cmd = parser.parse(options, args)
    } catch {
      case e: ParseException =>
        printHelpWithError(e)
    }
    cmd
  }
}

