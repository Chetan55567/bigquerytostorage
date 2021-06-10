import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import re
import logging

class Dataflow:
    def parsedata(self,inputdata):
        values = re.split(',',re.sub('\r\n','',re.sub('"','',inputdata)))
        row = dict(zip(('Name','Emp_ID','Role','ProjectAssigned','Rating','Comments'),values))
        return row

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input')
        parser.add_value_provider_argument('--output',default='innate-infusion-306605:demo.demotable')

# Create the options object
options = PipelineOptions()
# Specifying that the options expected come from our custom options class
my_options = options.view_as(MyOptions)
# Create the pipeline - simple read/write transforms

def run(argv=None):
    storedata = Dataflow()

    p = beam.Pipeline(options=options)

    (p | 'Read From a File' >> beam.io.ReadFromText(my_options.input,skip_header_lines=1)
    | 'String to BigQuery' >> beam.Map(lambda s: storedata.parsedata(s))
    | 'Write to BigQuery' >> beam.io.WriteToBigQuery(my_options.output,schema='Name:STRING,Emp_ID:STRING,Role:STRING,ProjectAssigned:STRING,Rating:STRING,Comments:STRING',create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
    )
    p.run().wait_until_finish()

    b = beam.Pipeline(options=options)
    (b | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(table='innate-infusion-306605:demo.demotable')
       | 'Get VAlues' >> beam.Map(lambda x: list(x.values()))
       | 'Convert CSV' >>  beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
       | 'Write_to_GCS' >> beam.io.WriteToText('gs://testbucketofgcp2/results/output', file_name_suffix='.csv', header='Name,Emp_ID,Role,ProjectAssigned,Rating,Comments')
            )
    b.run()

if __name__=='__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
