package Bench;// ============================================================================
//
// Copyright (C) 2006-2019 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.io.aws.s3.S3FileSystemRegistrar;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ReadText {

    public static void main(String[] args) {
        int nbExec = 25;
        int skipFirst = 5;

        List<Long> durations = new ArrayList<>();
        for(int i = 0; i < nbExec; i++) {
            long start = System.currentTimeMillis();
        /*PipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        runCopyFile(options);*/

            //runCountLocal();
            runCountS3();

            long end = System.currentTimeMillis();
            if(i >= skipFirst) {
                durations.add((end - start));
            }
        }
        System.out.println("* Execution duration :");
        long sum = 0;
        for(Long d : durations) {
            System.err.println(d);
            sum += d;
        }
        System.out.println("* Average : " + (sum / durations.size()));
        System.out.println("End.");
    }

    private static Properties loadConfig(){
        Properties prop = new Properties();
        try(InputStream is = new FileInputStream(new File("aws.conf"))){
            prop.load(is);
        }
        catch(Exception e){
            System.err.println("Err : "+e.getMessage());
            e.printStackTrace();
        }

        return prop;
    }

    private static void displayLoadedModules(){
        List<Module> modules = ObjectMapper.findModules(ReflectHelpers.findClassLoader()); // To check if AWS module is loaded
        for(Module m : modules){
            System.out.println("Loaded module : "+m.getModuleName());
        }
    }

    private static void displayRegistredFilesystems(PipelineOptions opts){
        // Check available file system

        // The explicit registration is not needed it done by the pipeline
        // in this case it is just to get the list af available filesystem
        S3FileSystemRegistrar fsr = new S3FileSystemRegistrar();
        Iterable<FileSystem> fileSystems = fsr.fromOptions(opts);
        for(FileSystem fs : fileSystems){
            System.out.println("Registered filesystem in pipeline  options :" + fs.toString());
        }
    }

    private static void runCountLocal(){
        //PipelineOptions noOption = PipelineOptionsFactory.create();

        // Due to beam bug need to give aws region
        // since I have  'beam-sdks-java-io-amazon-web-services' in dependencies
        S3Options s3o = PipelineOptionsFactory.as(S3Options.class);
        s3o.setAwsRegion("eu-west-1");

        runCount(s3o, //noOption,
                "C:\\Devx\\Tmp\\Resources\\persons.csv");
    }

    private static void runCountS3(){
        // Configure AWS credentials
        Properties prop = loadConfig();
        BasicAWSCredentials baseCred = new BasicAWSCredentials(prop.getProperty("access_key"), prop.getProperty("secret_key"));
        System.out.println("Credentials : " + prop.getProperty("access_key") +"/"+ prop.getProperty("secret_key"));

        AWSStaticCredentialsProvider cred = new AWSStaticCredentialsProvider(baseCred);
        S3Options s3o = PipelineOptionsFactory.as(S3Options.class);
        s3o.setAwsRegion(prop.getProperty("region"));
        s3o.setS3UploadBufferSizeBytes(5_242_880);
        s3o.setAwsCredentialsProvider(cred);

        runCount(s3o, "s3://ypiel/beam/persons.csv");
    }

    private static void runCount(PipelineOptions options, String pattern) {
        Integer zero = new Integer(0);
        /*displayLoadedModules();
        displayRegistredFilesystems(options);*/

        DirectOptions directOptions = options.as(DirectOptions.class);
        directOptions.setEnforceEncodability(false);
        directOptions.setEnforceImmutability(false);
        directOptions.setTargetParallelism(Runtime.getRuntime().availableProcessors());

        // Create the pipeline
        Pipeline p = Pipeline.create(options);
        p.apply("Configure", FileIO.match()
                .filepattern(pattern))
                .apply("Select files", FileIO.readMatches())
                .apply("Read Files Content",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                                .via((FileIO.ReadableFile f) -> {
                                    try {
                                        return KV.of(f.getMetadata().resourceId().toString(),
                                                f.readFullyAsUTF8String());
                                    } catch (IOException e) {
                                        System.out.println("Can't build pipeline : " + e.getMessage());
                                        e.printStackTrace();
                                    }
                                    return KV.of("", null); // /dev/null
                                }))
                .apply("Do nothing",
                        new PTransform<PCollection<KV<String, String>>, PCollection<Integer>>() {

                            @Override
                            public PCollection<Integer> expand(PCollection<KV<String, String>> input) {
                                return (PCollection) input.apply(
                                        ParDo.of(new DoFn<KV<String, String>, Integer>() {

                                            @ProcessElement
                                            public void processElement(
                                                    @Element
                                                            KV<String, String> elts,
                                                    OutputReceiver<Integer> out) throws Exception {

                                                // do nothing
                                                out.output(zero);
                                            }
                                        }));
                            }
                        });

        // Execute the pipeline
        p.run().waitUntilFinish();

    }
}
