function dump_obj(myObject) {
    var s = "";
    for (var property in myObject) {
        s = s + "\n "+property +": " + myObject[property] ;
    }
    alert(s);
}

function obj_type(obj) {
    alert(Object.prototype.toString.call(obj));
}

function postNewAssignment() {
    var name = $("#assignment_name").val();
    var configuration = $("#assignment_body").val();
    if (name.length === 0) {
        alert("assignment name can't be empty!");
        return;
    }
    if (configuration.length === 0) {
        alert("assignment body can't be empty!");
        return;
    }
    $.ajax({
        type: 'post',
        url: '/api/zhuque/' + name,
        data: configuration,
        dataType: "json",
        success: function (data) {
            var state = data.state;
            if (state === 400) {
                alert(data.body);
                return;
            }
            alert("create assignment success!");
        },
        error: function () {
            alert("unknown error!");
        }
    });
}

function deleteAssignment() {
    var name = $("#deleteAssignmentName").val();
    if (name.length === 0) {
        alert("assignment name can't be null");
        return;
    }
    $.ajax({
        type: 'delete',
        url: '/api/zhuque/' + name,
        success: function (data) {
            var state = data.state;
            if (state === 404) {
                alert("assignment not exist");
            } else if (state === 200) {
                alert("delete success");
            } else {
                alert("unknown error; delete failed;");
            }
        },
        error: function () {
            alert("unknown error");
        }
    });
}

function getAllAssignment() {
    $.ajax({
        type: 'get',
        url: "/api/zhuque",
        success: function (data) {
            var assignments = data.body;
            var body;
            if (assignments.length === 0) {
                body = "There is no Assignment;";
            } else {
                body = "<table>";
                for (var i = 0; i < assignments.length; i++) {
                    body += "<tr>";
                    body += "<th> id </th>";
                    body += "<th>" + (i + 1) + "</th>";
                    body += "<th> name </th>";
                    body += "<th>" + assignments[i] + "</th>";
                    body += "</tr>";
                }
                body += "</table>";
            }
            $("#showAllAssignmentDiv").html("<h3>Assignments:</h3> <br/>" + body);
        },
        error: function () {
            alert("unknown error; get Assignment failed!");
        }
    });
}

function displayAssignment() {
    var name = $("#displayAssignmentName").val();
    if (name.length === 0) {
        alert("assignment name can't empty");
        return;
    }
    $.ajax({
        type: 'get',
        url: '/api/zhuque/' + name,
        success: function (data) {
            var state = data.state;
            if (state === 404) {
                alert("assignment not exist!");
            } else if (state === 200) {
                //TODO: display
                alert(data.body);
            } else {
                alert("unknown error!");
            }
        },
        error: function () {
            alert("unknown error!");
        }
    });
}

$(document).ready(function () {
    $("#homePage").click(function () {
        location.href = "/";
    });
    $("#newAssignmentPage").click(function () {
        location.href = "/new";
    });
    $("#allAssignmentPage").click(function () {
        location.href = "/all";
    });
    $("#displayAssignmentPage").click(function () {
        location.href = "/display";
    });
    $("#deleteAssignmentPage").click(function () {
        location.href = "/delete";
    });

    $("#newAssignmentButton").click(function () {
        postNewAssignment();
    });
    $("#deleteAssignmentButton").click(function () {
        deleteAssignment();
    });
    $("#allAssignmentButton").click(function () {
        getAllAssignment();
    });
    $("#displayAssignmentButton").click(function () {
        displayAssignment();
    });
});