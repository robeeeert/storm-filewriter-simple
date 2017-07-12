package rob;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Map;

/**
 * Created by rob on 10.07.17.
 *
 */
public class Topology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Config conf = new Config();
        conf.setNumWorkers(1);
        conf.setDebug(true);
        conf.setMaxSpoutPending(5000);
        conf.setNumEventLoggers(1);

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("myspout", new MySpout());
        topologyBuilder.setBolt("mybolt", new MyBolt("foo.txt"))
                .shuffleGrouping("myspout");

        StormSubmitter.submitTopology("foobar", conf, topologyBuilder.createTopology());
    }
}

class MySpout extends BaseRichSpout{
    private SpoutOutputCollector spoutOutputCollector;
    private long i;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.i = 0L;
    }

    public void nextTuple() {
        spoutOutputCollector.emit(Collections.singletonList((Object) i++));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("i"));
    }
}

class MyBolt extends BaseRichBolt {
    private final String filename;
    private transient PrintWriter printWriter;

    MyBolt(String filename) {
        this.filename = filename;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            printWriter = new PrintWriter(new FileWriter(filename));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple tuple) {
        long i = tuple.getLongByField("i");
        printWriter.println(i);
        printWriter.flush();
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}