HBase Cluster on Amazon EC2 Using Whirr
--------------------------------------
March 9, 2012

[Whirr][whirr] is an apache project for helping setup cloud services on different providers. This walks you through the process of using Whirr to set-up hbase on Amazon EC2. This is based on the blog posts at [1] and [2]

1. Make sure you have an account on Amazon web services

2. Download whirr 0.71  

        wget http://apache.proserve.nl//whirr/whirr-0.7.1/whirr-0.7.1.tar.gz
        tar -xzf whirr-0.7.1.tar.gz

3. Add your AWS_ACCESS_KEY_ID and AWS_ACCESS_KEY_ID as environment variables. You'll find them in the security credentials page on aws-portal.amazon.com

        export AWS_ACCESS_KEY_ID=XXXXXX
        export AWS_SECRET_ACCESS_KEY=XXXXX

4. Generate a set of password less cryptographic keys

        ssh-keygen -t rsa -P ''
        Generating public/private rsa key pair.
        Enter file in which to save the key (/Users/bob/.ssh/id_rsa): whirr_rsa

5. From the whirr-0.7.1 installation directory,  copy the hbase-ec2.properties from the recipes directory and modify it to use your newly created keys. The properties file has details

        cp ./recipes/hbase-ec2.properties ./

6. Test that whirr works and then launch your cluster

        ./bin/whirr
        Usage: whirr COMMAND [ARGS]
        where COMMAND may be one of:
        .....
        
        ./bin/whirr launch-cluster --config hbase-ec2.properties

7. Success! Tons of log information will scroll by. You need to find the proxy server line so you can easily access the hbase installation. It will look like:

        /Users/bob/.whirr/hbase/hbase-proxy.sh

8. To log-in run the following (replacing bob and the server name):

        /Users/pgroth/.whirr/hbase/hbase-proxy.sh
        Running proxy to HBase cluster at ec2-75-xx-xx-xx.compute-1.amazonaws.com. Use Ctrl-c to quit

        #From another terminal you can ssh-in as follows

        ssh -i /Users/bob/.ssh/whirr_rsa bob@ec2-75-xx-xx-xx.compute-1.amazonaws.com

9. Once you've logged in you'll find base in the /usr/local/hbase-0.90.4 directory you can then use the standard hbase shell commands to investigate your cluster. See [HBase Shell Commands][3]

10. Watch out: the default properties launch a 5 node cluster running extra large instances. It's pretty big so may cost you more than you want if your just testing


[whirr]: http://whirr.apache.org/
[1]: http://www.bigfastblog.com/run-the-latest-whirr-and-deploy-hbase-in-minutes
[2]: http://dal-cloudcomputing.blogspot.com/2011/06/how-to-set-up-hadoop-and-hbase-together.html
[3]: http://wiki.apache.org/hadoop/Hbase/Shell "HBase Shell Commands"
