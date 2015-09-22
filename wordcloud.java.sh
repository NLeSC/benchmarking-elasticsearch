for x in {4..5}
do
time ../../python/Python-2.7.9/python wordcloud.java.py java_unthreaded_test$x
done
for x in {1..4}
do
time ../../python/Python-2.7.9/python wordcloud.java2.py java_threaded_test$x
done
