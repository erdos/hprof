(defproject io.github.erdos/hprof "0.1.0-SNAPSHOT"
  :description "Java heap dump analyzer in Clojure"
  :url "https://github.com/erdos/hprof"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [datalevin "0.5.12"]
                 [io.replikativ/datahike "0.3.6"]]
  :main erdos.hprof
  :repl-options {:init-ns erdos.hprof})
