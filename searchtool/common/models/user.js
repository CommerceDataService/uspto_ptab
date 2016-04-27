var config = require('../../server/config.json'),
    path = require('path');

module.exports = function( user ) {
  console.log('> ************REG ********');
  
  //send password reset link when requested
  user.on('resetPasswordRequest', function(info) {
    var url = 'http://' + config.host + ':' + config.port + '/reset-password';
    var html = 'Greetings -<br><br>We understand you lost your TFSPets password. Click <a href="' + url + '?access_token=' +
        info.accessToken.id + '">here</a> to reset your password <br><br>Thanks <br><br>This email was sent by:<br>The Filling Station Pet Supplies<br>10115 SW Nimbus Avenue<br>Tigard, OR 97223<br>503-352-4269<br>';

    user.app.models.Email.send({
      to: info.email,
      from: info.email,
      subject: 'TFSPets Password Reset',
      html: html
    }, function(err) {
      if (err) return console.log('> error sending password reset email');
      console.log('> sending password reset email to:', info.email);
    });
  });
};
