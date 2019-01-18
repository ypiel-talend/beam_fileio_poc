package fileio;// ============================================================================
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
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

public class FileioS3 {

    public static void main(String[] args) {
        /*PipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        runCopyFile(options);*/

        runCountLocal();
        //runCountS3();
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
        PipelineOptions noOption = PipelineOptionsFactory.create();
        runCount(noOption, "C:\\Devx\\Tmp\\people.csv");
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

        runCount(s3o, "s3://ypiel/csv/people.csv");
    }

    private static void runCount(PipelineOptions options, String pattern) {
        /*displayLoadedModules();
        displayRegistredFilesystems(options);*/

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
                .apply("Calculate size",
                        new PTransform<PCollection<KV<String, String>>, PCollection<KV<String, Integer>>>() {

                            @Override
                            public PCollection<KV<String, Integer>> expand(PCollection<KV<String, String>> input) {
                                return (PCollection) input.apply(
                                        ParDo.of(new DoFn<KV<String, String>, KV<String, Integer>>() {

                                            @ProcessElement
                                            public void processElement(
                                                    @Element
                                                            KV<String, String> elts,
                                                    OutputReceiver<KV<String, Integer>> out) throws Exception {
                                                String fileName = elts.getKey();
                                                Integer fileSize = elts.getValue().length();

                                                out.output(KV.of(fileName, fileSize));
                                            }
                                        }));
                            }
                        })
                .apply("CSV format", MapElements.into(TypeDescriptors.strings())
                        .via((KV<String, Integer> info) -> info.getKey() + "," + info.getValue()))
                .apply(FileIO.<String>write().via(TextIO.sink()).to("c:\\beamout"));

        // Execute the pipeline
        p.run().waitUntilFinish();

    }
}
