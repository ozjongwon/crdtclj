(defproject crdt "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [org.automerge/automerge "0.0.7"]
                 ]
  :repl-options {;;:init-ns crdt.core
                 :init-ns crdt.automerge
                 :init (init-lib)}
  :jvm-opts ["-Djava.library.path=/home/jc/Work/crdt/target/native/x86_64-unknown-linux-gnu/"
             "--add-opens" "java.base/java.lang=ALL-UNNAMED"
             "--enable-native-access=ALL-UNNAMED"]

  :java-source-paths ["src/java"]
  ;;  :javac-options ["-target" "1.8" "-source" "1.8"]
  :profiles {:precomp {:source-paths ["src/java"]
                       :aot [org.automerge] } }
  ;;  :target-path "target"
  ;;  :compile-path "target/classes"
  ;; :compile {:java-source-paths ["src/java"]
  ;;           :javac-options ["-d" "target/classes"]
  ;;           :output-to "target/classes"}

  ;; :native-path "/home/jc/Work/crdt/target/native/x86_64-unknown-linux-gnu/"
  ;;:native-dependencies [[libautomerge_jni "libautomerge_jni.so"]]
  )
