require 'java'

java_import 'java.util.concurrent.Executor'
java_import 'java.util.concurrent.Executors'
java_import 'java.util.concurrent.CountDownLatch'
java_import 'java.lang.Runnable'

class Concurrent

  def self.run_jobs(h_job_tree)
  
    h_job_tree.each do |s_job_name, s_job_type|
    
      if s_job_type == "sequential"
      
        o_job1 = Job.new("#{s_job_name}, 1", nil)
        o_job1.run()
        
        o_job2 = Job.new("#{s_job_name}, 2", nil)
        o_job2.run()
        
        o_job3 = Job.new("#{s_job_name}, 3", nil)
        o_job3.run()
      
      else # concurrent
      
        n_thread_count = 2
        n_sub_job_count = 3
        
        o_signal = CountDownLatch.new(n_sub_job_count)
        o_job1 = Job.new("#{s_job_name}, 1", o_signal)
        o_job2 = Job.new("#{s_job_name}, 2", o_signal)
        o_job3 = Job.new("#{s_job_name}, 3", o_signal)
        
        o_executor = Executors.new_fixed_thread_pool(n_thread_count)
        o_executor.execute(o_job1)
        o_executor.execute(o_job2)
        o_executor.execute(o_job3)
        o_executor.shutdown()
        o_signal.await()
        
      end
    
    end
  
  end
  
end

class Job
  include Runnable
  
  attr :job_name
  attr :done_signal
  
  def initialize(s_job_name, o_done_signal)
    @job_name = s_job_name
    @done_signal = o_done_signal
  end
  
  def run()
    puts "job #{@job_name} started"
    sleep rand() # sleep for random (between 0 and 1 secs)
    @done_signal.count_down() unless done_signal.nil?
    puts "job #{@job_name} finished"
  end
  
end

h_job_tree = {}
h_job_tree["exe 1, parallel 1"] = "sequential"
h_job_tree["exe 1, parallel 16"] = "sequential"
h_job_tree["exe 3, parallel 1"] = "concurrent"
h_job_tree["exe 3, parallel 16"] = "concurrent"

Concurrent.run_jobs(h_job_tree)
