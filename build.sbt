import Dependencies._

showCurrentGitBranch

git.useGitDescribe := true

lazy val commonSettings = Seq(
  organization := "org.hathitrust.htrc",
  organizationName := "HathiTrust Research Center",
  organizationHomepage := Some(url("https://www.hathitrust.org/htrc")),
  scalaVersion := "2.11.8",
  scalacOptions ++= Seq(
    "-feature",
    "-language:postfixOps",
    "-language:implicitConversions",
    "-target:jvm-1.8"
  ),
  resolvers ++= Seq(
    "I3 Repository" at "http://nexus.htrc.illinois.edu/content/groups/public",
    Resolver.mavenLocal
  ),
  packageOptions in (Compile, packageBin) += Package.ManifestAttributes(
    ("Git-Sha", git.gitHeadCommit.value.getOrElse("N/A")),
    ("Git-Branch", git.gitCurrentBranch.value),
    ("Git-Version", git.gitDescribedVersion.value.getOrElse("N/A")),
    ("Git-Dirty", git.gitUncommittedChanges.value.toString),
    ("Build-Date", new java.util.Date().toString)
  )
)

lazy val `find-citations` = (project in file(".")).
  enablePlugins(GitVersioning, GitBranchPrompt, JavaAppPackaging).
  settings(commonSettings: _*).
  //settings(spark("2.1.0"): _*).
  settings(spark_dev("2.1.0"): _*).
  settings(
    name := "find-citations",
    description := "Finds citations in a corpus based on author last name and/or short title matches",
    licenses += "Apache2" -> url("http://www.apache.org/licenses/LICENSE-2.0"),
    libraryDependencies ++= Seq(
      "org.hathitrust.htrc"           %% "pairtree-to-text"     % "5.1.2",
      "org.hathitrust.htrc"           %% "scala-utils"          % "2.1",
      "org.hathitrust.htrc"           %% "spark-utils"          % "1.0.2",
      "com.typesafe.play"             %% "play-json"            % "2.5.12"
        exclude("com.fasterxml.jackson.core", "jackson-databind")
        exclude("ch.qos.logback", "logback-classic"),
      "org.rogach"                    %% "scallop"              % "2.1.0",
      "com.jsuereth"                  %% "scala-arm"            % "2.0",
      "com.gilt"                      %% "gfc-time"             % "0.0.7",
      "com.github.nscala-time"        %% "nscala-time"          % "2.16.0",
      "ch.qos.logback"                %  "logback-classic"      % "1.2.1",
      "org.codehaus.janino"           %  "janino"               % "3.0.6",
      "org.scalatest"                 %% "scalatest"            % "3.0.1"      % Test
    )
  )
