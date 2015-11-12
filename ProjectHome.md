These tasks allow you to move files to and from Amazon's S3 service, and other tasks will be added to interface with other services as well.

## Dependencies ##
  * [Jets3t](http://jets3t.s3.amazonaws.com/downloads.html)
  * [commons-httpclient](http://hc.apache.org/downloads.cgi)
  * [commons-codec](http://commons.apache.org/downloads/download_codec.cgi)

## Example Usage ##
NOTE: This initial release is very basic. More features and task will be added as time goes on.

This example shows definition of the task, using a defined classpath ref. The upload target first loads a property file which contains the AWS access id and secret key. The task example simply uploads the contents of the lib directory, but it would upload any files that you specify.
```
<taskdef name="S3Upload" classname="dak.ant.taskdefs.S3Upload">
	<classpath refid="classpath.compile"/>
</taskdef>

<target name="upload">
	<property file="${basedir}/aws.properties"/>
	<S3Upload verbose="true"
			accessId="${aws.accessId}"
			secretKey="${aws.secretKey}"
			bucket="lifeguard"
			publicRead="true">
		<fileset dir="${lib}" includes="**/*.jar"/>
	</S3Upload>
</target>
```